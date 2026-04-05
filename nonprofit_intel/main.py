from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.panel import Panel

from processor import LeadQualifyingEngine, QualifiedLead
from scraper import NonProfitScraper, RawOrganizationData

console = Console()

load_dotenv()

OUTPUT_DIR: Path = Path("output")
DEFAULT_MAX_PAGES: int = 3
DEFAULT_MIN_SCORE: int = 4
DEFAULT_CONCURRENCY: int = 4


def _resolve_config() -> dict[str, Any]:
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if not gemini_key:
        console.print("[FATAL] GEMINI_API_KEY environment variable is not set.", style="bold red")
        sys.exit(1)

    return {
        "gemini_api_key": gemini_key,
        "max_pages": int(os.getenv("SCRAPER_MAX_PAGES", DEFAULT_MAX_PAGES)),
        "min_lead_score": int(os.getenv("MIN_LEAD_SCORE", DEFAULT_MIN_SCORE)),
        "concurrency": int(os.getenv("SCRAPER_CONCURRENCY", DEFAULT_CONCURRENCY)),
        "output_dir": Path(os.getenv("OUTPUT_DIR", OUTPUT_DIR)),
        "custom_urls": _parse_custom_urls(os.getenv("CUSTOM_TARGET_URLS", "")),
        "postgres_dsn": os.getenv("POSTGRES_DSN", ""),
    }


def _parse_custom_urls(raw: str) -> list[str]:
    if not raw.strip():
        return []
    return [url.strip() for url in raw.split(",") if url.strip().startswith("http")]


async def _collect_raw_leads(config: dict[str, Any]) -> list[RawOrganizationData]:
    console.print(f"[SCRAPER] Starting async scrape - max_pages={config['max_pages']}", style="bold cyan")
    raw_leads: list[RawOrganizationData] = []

    async with NonProfitScraper(
        max_pages=config["max_pages"],
        concurrency=config["concurrency"],
    ) as scraper:
        if config["custom_urls"]:
            console.print(f"[SCRAPER] Mode: Custom URLs ({len(config['custom_urls'])} targets)", style="yellow")
            async for org in scraper.scrape_custom_urls(config["custom_urls"]):
                raw_leads.append(org)
                console.print(f"[SCRAPER]  + Captured: {org.name}", style="green")
        else:
            console.print("[SCRAPER] Mode: Default NGO directory pagination", style="yellow")
            async for org in scraper.scrape():
                raw_leads.append(org)
                console.print(f"[SCRAPER]  + Captured: {org.name}", style="green")

    console.print(f"[SCRAPER] Total raw organizations collected: {len(raw_leads)}", style="bold green")
    return raw_leads


async def _qualify_leads(
    raw_leads: list[RawOrganizationData],
    config: dict[str, Any],
) -> list[QualifiedLead]:
    if not raw_leads:
        console.print("[PROCESSOR] No raw leads to process.", style="yellow")
        return []

    console.print(f"[PROCESSOR] Qualifying {len(raw_leads)} leads via Gemini 1.5 Flash...", style="bold cyan")
    engine = LeadQualifyingEngine(
        api_key=config["gemini_api_key"],
        max_concurrent_requests=config["concurrency"],
        console=console,
    )

    qualified = await engine.qualify_stream(
        raw_leads,
        min_score_threshold=config["min_lead_score"],
    )
    used_fallback = False

    if not qualified:
        used_fallback = True
        console.print(
            "[PROCESSOR] No leads met the configured minimum score. Falling back to score >= 1.",
            style="bold yellow",
        )
        fallback = await engine.qualify_stream(raw_leads, min_score_threshold=1)
        qualified = fallback[:25]

    if used_fallback:
        console.print(f"[PROCESSOR] + Leads exported from fallback set: {len(qualified)}", style="bold green")
    else:
        console.print(f"[PROCESSOR] + Qualified leads (score >= {config['min_lead_score']}): {len(qualified)}", style="bold green")
    return qualified


def _serialize_leads(leads: list[QualifiedLead]) -> list[dict[str, Any]]:
    return [lead.model_dump() for lead in leads]


def _build_output_payload(
    leads: list[QualifiedLead],
    config: dict[str, Any],
) -> dict[str, Any]:
    scores = [lead.lead_score for lead in leads]
    sector_distribution: dict[str, int] = {}
    for lead in leads:
        sector_distribution[lead.target_sector] = (
            sector_distribution.get(lead.target_sector, 0) + 1
        )

    return {
        "run_metadata": {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "total_qualified_leads": len(leads),
            "min_score_filter": config["min_lead_score"],
            "average_lead_score": round(sum(scores) / len(scores), 2) if scores else 0,
            "max_lead_score": max(scores) if scores else 0,
            "sector_distribution": sector_distribution,
        },
        "qualified_leads": _serialize_leads(leads),
    }


async def _export_to_json(payload: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"qualified_leads_{timestamp}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    console.print(f"[EXPORTER] + JSON export saved: {output_path}", style="bold green")
    return output_path


async def _export_to_postgres(
    leads: list[QualifiedLead],
    dsn: str,
) -> None:
    try:
        import asyncpg
    except ImportError:
        console.print("[EXPORTER] asyncpg is not installed. Skipping PostgreSQL export.", style="bold yellow")
        return

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS qualified_leads (
            id              SERIAL PRIMARY KEY,
            organization_name   TEXT NOT NULL,
            mission_statement   TEXT,
            target_sector       TEXT,
            lead_score          SMALLINT,
            outreach_trigger    TEXT,
            website             TEXT,
            source_url          TEXT,
            created_at          TIMESTAMPTZ DEFAULT NOW()
        );
    """

    insert_sql = """
        INSERT INTO qualified_leads
            (organization_name, mission_statement, target_sector, lead_score,
             outreach_trigger, website, source_url)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    """

    try:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(create_table_sql)
            records = [
                (
                    lead.organization_name,
                    lead.mission_statement,
                    lead.target_sector,
                    lead.lead_score,
                    lead.outreach_trigger,
                    lead.website,
                    lead.source_url,
                )
                for lead in leads
            ]
            await conn.executemany(insert_sql, records)
            console.print(f"[EXPORTER] + {len(records)} leads inserted into PostgreSQL.", style="bold green")
        finally:
            await conn.close()
    except Exception as e:
        console.print(f"[EXPORTER] Error inserting into PostgreSQL: {e}", style="bold red")


def _print_summary(payload: dict[str, Any]) -> None:
    meta = payload["run_metadata"]
    leads = payload["qualified_leads"]

    # Metadata Panel
    metadata_table = Table(title="RUN METADATA", title_style="bold cyan", show_header=False, box=None, padding=(0, 1))
    metadata_table.add_row("Timestamp", meta['timestamp_utc'])
    metadata_table.add_row("Qualified Leads", f"[bold green]{meta['total_qualified_leads']}[/bold green]")
    metadata_table.add_row("Average Score", f"[bold yellow]{meta['average_lead_score']}/10[/bold yellow]")
    metadata_table.add_row("Max Score", f"[bold magenta]{meta['max_lead_score']}/10[/bold magenta]")
    
    sector_info = ", ".join([f"{k}: [bold blue]{v}[/bold blue]" for k, v in meta['sector_distribution'].items()])
    metadata_table.add_row("Sector Distribution", sector_info)
    
    console.print(Panel(metadata_table, border_style="cyan", expand=False))

    # Top 5 Leads Table
    top_leads = sorted(leads, key=lambda x: x["lead_score"], reverse=True)[:5]
    if top_leads:
        top_table = Table(title="TOP 5 QUALIFIED LEADS", title_style="bold green")
        top_table.add_column("Rank", style="bold cyan", width=5)
        top_table.add_column("Organization", style="bold white")
        top_table.add_column("Score", style="bold yellow", justify="center")
        top_table.add_column("Sector", style="magenta")
        top_table.add_column("Outreach Trigger", style="dim")

        score_colors = {10: "bright_green", 9: "green", 8: "yellow", 7: "yellow", 6: "orange1", 5: "orange3"}
        
        for i, lead in enumerate(top_leads, 1):
            score = lead['lead_score']
            color = score_colors.get(score, "red")
            score_display = f"[{color}]{score}/10[/{color}]"
            trigger = lead['outreach_trigger'][:80] + "..." if len(lead['outreach_trigger']) > 80 else lead['outreach_trigger']
            top_table.add_row(str(i), lead['organization_name'], score_display, lead['target_sector'], trigger)

        console.print(top_table)

    # All Leads Summary Table
    all_leads_table = Table(title="ALL QUALIFIED LEADS", title_style="bold blue")
    all_leads_table.add_column("#", style="bold cyan")
    all_leads_table.add_column("Organization", style="bold white")
    all_leads_table.add_column("Score", style="bold yellow", justify="center")
    all_leads_table.add_column("Sector", style="magenta")
    all_leads_table.add_column("Website", style="blue")

    for i, lead in enumerate(leads, 1):
        score = lead['lead_score']
        color = {10: "bright_green", 9: "green", 8: "yellow", 7: "yellow", 6: "orange1", 5: "orange3"}.get(score, "red")
        score_display = f"[{color}]{score}/10[/{color}]"
        website = lead.get('website') or "N/A"
        all_leads_table.add_row(str(i), lead['organization_name'][:40], score_display, lead['target_sector'], website[:30])

    console.print(all_leads_table)
    console.print("\n[bold green]+ Pipeline execution completed successfully![/bold green]\n", justify="center")


async def run() -> None:
    config = _resolve_config()
    raw_leads = await _collect_raw_leads(config)
    qualified_leads = await _qualify_leads(raw_leads, config)
    output_payload = _build_output_payload(qualified_leads, config)
    await _export_to_json(output_payload, config["output_dir"])

    if config["postgres_dsn"] and qualified_leads:
        await _export_to_postgres(qualified_leads, config["postgres_dsn"])

    _print_summary(output_payload)


if __name__ == "__main__":
    asyncio.run(run())

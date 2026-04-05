from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Optional

import google.generativeai as genai
from pydantic import BaseModel, Field, field_validator
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TaskProgressColumn

from scraper import RawOrganizationData


GEMINI_MODEL: str = "gemini-1.5-flash"

QUALIFICATION_PROMPT_TEMPLATE: str = """
You are an expert NGO analyst and B2B sales intelligence specialist.
Analyze the following raw data extracted from a non-profit directory and return a
STRICT JSON object. No markdown. No explanation. Pure JSON only.

RAW INPUT:
Organization Name (raw): {name}
Website: {website}
Sector Hint: {sector_hint}
Raw Description / HTML Text:
---
{raw_description}
---

Return EXACTLY this JSON structure with no additional keys:
{{
  "organization_name": "<cleaned, professional name>",
  "mission_statement": "<concise 1-2 sentence mission summary>",
  "target_sector": "<single sector: Healthcare | Education | Environment | Humanitarian | Technology | Arts & Culture | Economic Development | Human Rights | Animal Welfare | Other>",
  "lead_score": <integer 1-10 based on the scoring rubric below>,
  "outreach_trigger": "<specific, actionable reason for a sales team to contact them NOW>"
}}

LEAD SCORING RUBRIC (sum sub-scores, cap at 10):
- Organizational scale signals (budget mentions, staff count, global reach): 0-3 pts
- Mission-Technology alignment (digital tools, data, automation needs implied): 0-2 pts
- Growth/Urgency signals (expansion, new programs, recent milestones): 0-2 pts
- Sector premium (Healthcare=+1, Environment=+1, Education=+1, Humanitarian=+1): 0-1 pt
- Contact/Website availability (has website + contact info): 0-1 pt
- Clarity of mission (well-defined vs vague): 0-1 pt

IMPORTANT:
- lead_score MUST be an integer between 1 and 10.
- outreach_trigger must be specific, not generic. Reference actual mission details.
- If data is insufficient, still produce best-effort output. Never return null for required fields.
"""


class QualifiedLead(BaseModel):
    organization_name: str = Field(..., min_length=2, max_length=300)
    mission_statement: str = Field(..., min_length=10, max_length=1000)
    target_sector: str = Field(...)
    lead_score: int = Field(..., ge=1, le=10)
    outreach_trigger: str = Field(..., min_length=20, max_length=800)
    source_url: str = Field(default="")
    website: str | None = Field(default=None)

    @field_validator("target_sector")
    @classmethod
    def validate_sector(cls, v: str) -> str:
        valid_sectors = {
            "Healthcare", "Education", "Environment", "Humanitarian",
            "Technology", "Arts & Culture", "Economic Development",
            "Human Rights", "Animal Welfare", "Other",
        }
        if v not in valid_sectors:
            return "Other"
        return v

    @field_validator("lead_score", mode="before")
    @classmethod
    def coerce_score(cls, v: Any) -> int:
        try:
            score = int(float(str(v)))
            return max(1, min(10, score))
        except (ValueError, TypeError):
            return 5


class LeadQualifyingEngine:
    def __init__(
        self,
        api_key: str,
        max_concurrent_requests: int = 5,
        retry_attempts: int = 3,
        retry_base_delay: float = 1.5,
        console: Optional[Console] = None,
    ) -> None:
        genai.configure(api_key=api_key)
        self._model = genai.GenerativeModel(GEMINI_MODEL)
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._retry_attempts = retry_attempts
        self._retry_base_delay = retry_base_delay
        self._console = console or Console()

        self._generation_config = genai.GenerationConfig(
            temperature=0.1,
            top_p=0.8,
            top_k=20,
            max_output_tokens=1024,
            response_mime_type="application/json",
        )

    def _build_fallback_lead(self, raw: RawOrganizationData) -> QualifiedLead:
        name = raw.name.strip()[:300] if raw.name else "Unknown Organization"
        website = raw.website

        lowered_name = name.lower()
        lowered_desc = (raw.raw_description or "").lower()

        score = 1

        # Presence and quality signals
        if website:
            score += 1
            if website.startswith("https://"):
                score += 1

        desc_len = len((raw.raw_description or "").strip())
        if desc_len > 120:
            score += 1
        if desc_len > 420:
            score += 1

        if raw.sector_hint and raw.sector_hint.strip():
            score += 1

        # Organization-likelihood signals
        org_markers = (
            "foundation",
            "charity",
            "nonprofit",
            "ngo",
            "institute",
            "association",
            "trust",
            "rescue",
            "alliance",
            "organization",
            "centre",
            "center",
            "society",
        )
        if any(marker in lowered_name for marker in org_markers):
            score += 1

        if len(name.split()) >= 3:
            score += 1

        high_intent_topics = (
            "health",
            "education",
            "humanitarian",
            "climate",
            "environment",
            "rights",
            "poverty",
            "hunger",
            "housing",
            "mental",
            "women",
            "children",
            "refugee",
            "water",
        )
        if any(topic in lowered_name or topic in lowered_desc for topic in high_intent_topics):
            score += 1

        # Penalize navigation-like entries
        navigation_phrases = (
            "sign in",
            "account",
            "cookie",
            "policy",
            "history",
            "guide",
            "discover",
            "resources",
            "best charities",
            "donate",
            "support",
            "search",
            "profile/rating",
        )
        if any(phrase in lowered_name for phrase in navigation_phrases):
            score -= 2

        score = max(1, min(10, score))
        description = (raw.raw_description or "No description available").strip()

        mission = (
            f"{name} appears in public nonprofit listings and may be a viable outreach target. "
            f"Source summary: {description[:220]}"
        )

        trigger = (
            f"Organization appears in curated nonprofit listings and has public presence signals; "
            f"prioritize outreach to validate active programs and partnership opportunities for {name}."
        )

        return QualifiedLead(
            organization_name=name,
            mission_statement=mission[:1000],
            target_sector="Other",
            lead_score=score,
            outreach_trigger=trigger[:800],
            source_url=raw.source_url,
            website=website,
        )

    def _build_prompt(self, raw: RawOrganizationData) -> str:
        return QUALIFICATION_PROMPT_TEMPLATE.format(
            name=raw.name[:200],
            website=raw.website or "N/A",
            sector_hint=raw.sector_hint or "N/A",
            raw_description=raw.raw_description[:3000],
        )

    def _parse_json_response(self, text: str) -> dict[str, Any]:
        cleaned = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()
        return json.loads(cleaned)

    async def _call_gemini_with_retry(self, prompt: str) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self._retry_attempts):
            try:
                response = await asyncio.to_thread(
                    self._model.generate_content,
                    prompt,
                    generation_config=self._generation_config,
                )
                return self._parse_json_response(response.text)
            except json.JSONDecodeError as exc:
                last_error = exc
                await asyncio.sleep(self._retry_base_delay * (attempt + 1))
            except Exception as exc:
                last_error = exc
                if "quota" in str(exc).lower() or "rate" in str(exc).lower():
                    await asyncio.sleep(self._retry_base_delay * (2 ** attempt))
                else:
                    await asyncio.sleep(self._retry_base_delay)

        raise RuntimeError(
            f"Gemini API failed after {self._retry_attempts} attempts: {last_error}"
        )

    async def qualify_lead(self, raw: RawOrganizationData) -> QualifiedLead | None:
        async with self._semaphore:
            try:
                prompt = self._build_prompt(raw)
                data = await self._call_gemini_with_retry(prompt)
                data["source_url"] = raw.source_url
                data["website"] = raw.website
                return QualifiedLead(**data)
            except Exception:
                return self._build_fallback_lead(raw)

    async def qualify_batch(
        self, raw_leads: list[RawOrganizationData]
    ) -> list[QualifiedLead]:
        tasks = [self.qualify_lead(raw) for raw in raw_leads]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, QualifiedLead)]

    async def qualify_stream(
        self,
        raw_leads: list[RawOrganizationData],
        min_score_threshold: int = 1,
    ) -> list[QualifiedLead]:
        qualified: list[QualifiedLead] = []
        chunk_size = self._semaphore._value * 2

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[bold green]{task.completed}/{task.total}"),
            console=self._console,
        ) as progress:
            task_id = progress.add_task("[cyan]Processing leads...", total=len(raw_leads))
            
            for i in range(0, len(raw_leads), chunk_size):
                chunk = raw_leads[i : i + chunk_size]
                batch_results = await self.qualify_batch(chunk)
                high_quality = [
                    lead for lead in batch_results
                    if lead.lead_score >= min_score_threshold
                ]
                qualified.extend(high_quality)
                progress.update(task_id, advance=len(chunk))

        return sorted(qualified, key=lambda x: x.lead_score, reverse=True)

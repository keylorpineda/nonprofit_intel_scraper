from __future__ import annotations

import asyncio
import random
import re
from dataclasses import dataclass, field
from typing import AsyncGenerator
from urllib.parse import urljoin, urlparse

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright


USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

VIEWPORT_PRESETS: list[dict[str, int]] = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1280, "height": 800},
]

NOISE_NAMES: set[str] = {
    "home",
    "about",
    "contact",
    "donate",
    "login",
    "sign in",
    "register",
    "privacy",
    "terms",
    "menu",
    "search",
    "read more",
    "learn more",
    "view all",
}

NOISE_PHRASES: tuple[str, ...] = (
    "sign in",
    "create an account",
    "donation history",
    "recurring donations",
    "cookie policy",
    "privacy statement",
    "manage options",
    "manage services",
    "read more",
    "knowledge base",
    "post a job",
    "search organizations",
    "where to give",
    "support charity navigator",
)

DONATION_DOMAINS: set[str] = {
    "give.charitynavigator.org",
    "giving.classy.org",
    "www.classy.org",
    "classy.org",
    "donate.stripe.com",
    "www.paypal.com",
    "gofundme.com",
}


@dataclass
class RawOrganizationData:
    name: str
    website: str | None
    raw_description: str
    source_url: str
    sector_hint: str | None = None
    extra_metadata: dict[str, str] = field(default_factory=dict)


class NonProfitScraper:
    BASE_URL: str = "https://www.ngoadvisor.net/ong/"
    PAGINATION_PARAM: str = "?page={page}"

    def __init__(
        self,
        max_pages: int = 5,
        concurrency: int = 3,
        min_delay_ms: int = 250,
        max_delay_ms: int = 700,
        headless: bool = True,
    ) -> None:
        self._max_pages = max_pages
        self._concurrency = concurrency
        self._min_delay_ms = min_delay_ms
        self._max_delay_ms = max_delay_ms
        self._headless = headless
        self._semaphore = asyncio.Semaphore(concurrency)
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None

    async def __aenter__(self) -> NonProfitScraper:
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _create_stealth_context(self) -> BrowserContext:
        if not self._browser:
            raise RuntimeError("Browser is not initialized")

        agent = random.choice(USER_AGENTS)
        viewport = random.choice(VIEWPORT_PRESETS)
        context = await self._browser.new_context(
            user_agent=agent,
            viewport=viewport,
            locale="en-US",
            timezone_id="America/New_York",
            java_script_enabled=True,
            ignore_https_errors=False,
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        return context

    async def _block_unnecessary_resources(self, route, request) -> None:
        if request.resource_type in {"image", "media", "font", "stylesheet"}:
            await route.abort()
        else:
            await route.continue_()

    async def _human_delay(self) -> None:
        await asyncio.sleep(random.randint(self._min_delay_ms, self._max_delay_ms) / 1000)

    async def _scroll_page(self, page: Page) -> None:
        for _ in range(3):
            await page.mouse.wheel(0, 2500)
            await asyncio.sleep(0.4)

    def _normalize_url(self, href: str | None, base_url: str) -> str | None:
        if not href:
            return None

        href = href.strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            return None

        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            return None
        return absolute

    def _is_plausible_org_name(self, value: str | None) -> bool:
        if not value:
            return False

        cleaned = re.sub(r"\s+", " ", value).strip()
        if len(cleaned) < 4 or len(cleaned) > 120:
            return False

        lowered = cleaned.lower()
        if lowered in NOISE_NAMES:
            return False
        if any(phrase in lowered for phrase in NOISE_PHRASES):
            return False
        if lowered.startswith(("donate to ", "best charities", "popular charities", "charities with")):
            return False

        words = re.findall(r"[a-zA-Z0-9&'-]+", lowered)
        if len(words) < 2:
            return False

        return True

    def _looks_like_org_profile_url(self, candidate_url: str, source_url: str) -> bool:
        parsed_candidate = urlparse(candidate_url)
        parsed_source = urlparse(source_url)
        path = parsed_candidate.path.lower()
        candidate_host = parsed_candidate.netloc.lower()
        source_host = parsed_source.netloc.lower()

        if any(x in path for x in ("/category/", "/tag/", "/topic/", "/search", "?page=")):
            return False

        # Domain-specific hard filters for known dynamic sources.
        if "charitynavigator.org" in candidate_host:
            # Keep only concrete charity profile URLs on Charity Navigator.
            if "/ein/" in path or "/charity-evaluator/" in path:
                return True
            return False

        if "idealist.org" in candidate_host:
            # Keep only nonprofit profile pages, not guides/tools/category pages.
            return "/en/nonprofit/" in path

        if "thedotgood.net" in candidate_host:
            # DotGood pages are mostly rankings/articles; avoid internal article pages.
            return False

        if any(
            token in path
            for token in (
                "charity",
                "nonprofit",
                "organization",
                "profile",
                "ngo",
                "directory",
            )
        ):
            return True

        # Allow external candidate URLs only when source is a known listing page.
        if source_host in {"thedotgood.net", "www.thedotgood.net"}:
            return parsed_candidate.netloc != parsed_source.netloc

        return False

    def _is_profile_url_allowed_for_scrape(self, profile_url: str) -> bool:
        parsed = urlparse(profile_url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()

        if "charitynavigator.org" in host:
            return "/ein/" in path or "/charity-evaluator/" in path

        if "idealist.org" in host:
            return "/en/nonprofit/" in path

        if "thedotgood.net" in host:
            return False

        return True

    def _is_preferred_organization_website(self, url: str, profile_host: str) -> bool:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()

        if not host or host == profile_host:
            return False
        if host in DONATION_DOMAINS:
            return False
        if any(x in path for x in ("/donate", "/donation", "/checkout", "/campaign")):
            return False
        if any(x in host for x in ("facebook.com", "twitter.com", "linkedin.com", "instagram.com", "youtube.com", "threads.net")):
            return False

        return True

    async def _extract_profile_urls(self, page: Page, listing_url: str) -> list[str]:
        profile_urls: set[str] = set()
        try:
            await page.goto(listing_url, wait_until="networkidle", timeout=45_000)
        except Exception:
            await page.goto(listing_url, wait_until="domcontentloaded", timeout=45_000)

        await self._scroll_page(page)
        await self._human_delay()

        link_candidates = await page.evaluate(
            """
            () => {
              const anchors = Array.from(document.querySelectorAll('a[href]'));
              return anchors.map((a) => ({
                text: (a.textContent || '').trim(),
                href: a.getAttribute('href') || '',
                title: (a.getAttribute('title') || '').trim(),
              }));
            }
            """
        )

        for candidate in link_candidates:
            href = self._normalize_url(candidate.get("href"), listing_url)
            if not href:
                continue

            if not self._looks_like_org_profile_url(href, listing_url):
                continue

            text = candidate.get("text") or candidate.get("title")
            path_lower = urlparse(href).path.lower()
            if self._is_plausible_org_name(text) or any(x in path_lower for x in ("/org/", "/ngo/", "/profile/")):
                profile_urls.add(href)

        return list(profile_urls)

    async def _safe_extract_text(self, page: Page, selectors: list[str]) -> str | None:
        for selector in selectors:
            try:
                el = await page.query_selector(selector)
                if not el:
                    continue

                text = (await el.inner_text() or "").strip()
                if text:
                    return text
            except Exception:
                continue
        return None

    async def _scrape_profile_page(self, profile_url: str) -> RawOrganizationData | None:
        if not self._is_profile_url_allowed_for_scrape(profile_url):
            return None

        async with self._semaphore:
            context = await self._create_stealth_context()
            try:
                page = await context.new_page()
                await page.route("**/*", self._block_unnecessary_resources)
                await page.goto(profile_url, wait_until="domcontentloaded", timeout=45_000)
                await self._human_delay()

                name = await self._safe_extract_text(page, ["h1", ".org-title", ".profile-title", "h2.name"])
                if not name:
                    title = await page.title()
                    name = title.split("|")[0].split("-")[0].strip()

                if not self._is_plausible_org_name(name):
                    return None

                base_domain = urlparse(profile_url).netloc
                external_links = await page.evaluate(
                    """
                    ({baseDomain}) => {
                      const links = Array.from(document.querySelectorAll('a[href]'));
                      return links.map((a) => a.href).filter((href) => {
                        try {
                          const u = new URL(href);
                          if (!(u.protocol === 'http:' || u.protocol === 'https:')) return false;
                          if (u.hostname.includes(baseDomain)) return false;
                          if (['facebook.com','twitter.com','linkedin.com','instagram.com','youtube.com'].some((s) => u.hostname.includes(s))) return false;
                          return true;
                        } catch {
                          return false;
                        }
                      });
                    }
                    """,
                    {"baseDomain": base_domain},
                )
                website = None
                for link in external_links:
                    if self._is_preferred_organization_website(link, base_domain):
                        website = link
                        break

                raw_description = await self._safe_extract_text(
                    page,
                    [".description", ".about", "#about", ".mission", "main p", "article p", "p"],
                )
                if not raw_description:
                    body_text = await page.inner_text("body")
                    raw_description = body_text[:2000] if body_text else f"Profile page: {profile_url}"

                return RawOrganizationData(
                    name=re.sub(r"\s+", " ", name).strip(),
                    website=website,
                    raw_description=raw_description.strip(),
                    source_url=profile_url,
                    sector_hint=None,
                )
            except Exception:
                return None
            finally:
                await context.close()

    async def _get_profile_urls_from_listing(self, listing_url: str) -> list[str]:
        async with self._semaphore:
            context = await self._create_stealth_context()
            try:
                page = await context.new_page()
                await page.route("**/*", self._block_unnecessary_resources)
                return await self._extract_profile_urls(page, listing_url)
            except Exception:
                return []
            finally:
                await context.close()

    def _build_page_urls(self) -> list[str]:
        urls: list[str] = []
        for page_num in range(1, self._max_pages + 1):
            if page_num == 1:
                urls.append(self.BASE_URL)
            else:
                urls.append(f"{self.BASE_URL}{self.PAGINATION_PARAM.format(page=page_num)}")
        return urls

    async def _crawl_from_listings(self, urls: list[str]) -> list[RawOrganizationData]:
        profile_urls_set: set[str] = set()

        listing_tasks = [self._get_profile_urls_from_listing(url) for url in urls]
        listing_results = await asyncio.gather(*listing_tasks, return_exceptions=True)
        for result in listing_results:
            if isinstance(result, Exception):
                continue
            profile_urls_set.update(result)

        profile_tasks = [self._scrape_profile_page(profile_url) for profile_url in profile_urls_set]
        profile_results = await asyncio.gather(*profile_tasks, return_exceptions=True)

        cleaned: list[RawOrganizationData] = []
        seen: set[str] = set()
        for result in profile_results:
            if isinstance(result, Exception) or result is None:
                continue
            key = re.sub(r"\W+", "", result.name.lower())
            if key and key not in seen:
                seen.add(key)
                cleaned.append(result)
        return cleaned

    async def scrape(self) -> AsyncGenerator[RawOrganizationData, None]:
        for item in await self._crawl_from_listings(self._build_page_urls()):
            yield item

    async def scrape_custom_urls(self, urls: list[str]) -> AsyncGenerator[RawOrganizationData, None]:
        for item in await self._crawl_from_listings(urls):
            yield item

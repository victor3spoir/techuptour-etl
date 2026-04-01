import json
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------


class CyberNews(BaseModel):
    title: str
    url: str
    image: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    published_date: Optional[str] = None
    fetched_at: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

BASE_URL = "https://thehackernews.com/"


def _abs(url: str, base: str) -> str:
    """Make a relative URL absolute."""
    if not url:
        return url
    if url.startswith("http"):
        return url
    return urljoin(base, url)


def _first_img(tag) -> Optional[str]:
    """Return the best src/data-src from an img tag."""
    if not tag:
        return None
    img = tag.find("img")
    if not img:
        return None
    for attr in ("data-src", "data-lazy-src", "src"):
        val = img.get(attr, "").strip()
        if val and not val.endswith("gif"):
            return val
    return None


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _parse_hacker_news(soup: BeautifulSoup, fetched_at: str) -> list[CyberNews]:
    """Parse The Hacker News articles using BeautifulSoup."""
    news_items: list[CyberNews] = []

    # Target main article containers
    cards = soup.select("div.mb-0.js-readmore")
    if not cards:
        cards = soup.find_all("div", class_=re.compile(r"post|article|news", re.I))[:20]

    for card in cards:
        # Title + link
        title_el = card.find("a", class_=re.compile(r"post-title|title|headline", re.I))
        if not title_el:
            title_el = card.find(["h2", "h3"])
            if title_el:
                title_el = title_el.find("a")
        if not title_el:
            title_el = card.find("a")

        if not title_el or not title_el.get_text(strip=True):
            continue

        title = title_el.get_text(strip=True)
        url = _abs(title_el.get("href", ""), BASE_URL)
        if not title or not url or url == BASE_URL:
            continue

        # Image
        image = _first_img(card)

        # Description
        desc_el = card.find("p", class_=re.compile(r"desc", re.I))
        if not desc_el:
            desc_el = card.find("p")
        description = desc_el.get_text(strip=True)[:200] if desc_el else None

        # Category/Tags
        category = None
        cat_el = card.find(
            ["span", "a"], class_=re.compile(r"label|tag|category", re.I)
        )
        if cat_el:
            category = cat_el.get_text(strip=True)

        # Published date
        published_date = None
        time_el = card.find("time")
        if time_el:
            published_date = time_el.get("datetime") or time_el.get_text(strip=True)
        else:
            date_match = re.search(
                r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}",
                card.get_text(),
            )
            if date_match:
                published_date = date_match.group(0)

        news_items.append(
            CyberNews(
                title=title,
                url=url,
                image=image,
                description=description,
                category=category,
                published_date=published_date,
                fetched_at=fetched_at,
            )
        )

    return news_items


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------


def fetch_cyber_news() -> list[CyberNews]:
    fetched_at = datetime.now(timezone.utc).isoformat()

    with httpx.Client(
        timeout=15,
        follow_redirects=True,
        headers=HEADERS,
    ) as client:
        response = client.get(BASE_URL)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    # DEBUG
    print(f"Response status: {response.status_code}", flush=True)
    print(f"HTML length: {len(response.text)} chars", flush=True)
    print(
        f"Found divs with 'mb-0 js-readmore': {len(soup.select('div.mb-0.js-readmore'))}",
        flush=True,
    )
    print(
        f"Found article containers: {len(soup.find_all(['article', 'div'], class_=re.compile(r'post|article|news', re.I)))}",
        flush=True,
    )

    # Parse
    news_items = _parse_hacker_news(soup, fetched_at)
    print(f"Parsed {len(news_items)} cyber news items", flush=True)

    return news_items


# ---------------------------------------------------------------------------
# Main - Kestra outputFiles pattern (writes cyber_news.json)
# ---------------------------------------------------------------------------


def main() -> None:
    news_items = fetch_cyber_news()
    output = [n.model_dump() for n in news_items]

    with open("cyber_news.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nFetched {len(output)} cyber security news from {BASE_URL}", flush=True)
    for n in news_items:
        date_str = f" | {n.published_date}" if n.published_date else ""
        cat_str = f" [{n.category}]" if n.category else ""
        print(f"  - {n.title}{cat_str}{date_str}", flush=True)


if __name__ == "__main__":
    main()

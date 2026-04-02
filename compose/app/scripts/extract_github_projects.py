import json
import re
from datetime import datetime, timezone
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel


class GitHubProject(BaseModel):
    owner: str
    name: str
    full_name: str
    url: str
    description: Optional[str] = None
    language: Optional[str] = None
    topics: list[str] = []
    stars: Optional[int] = None
    updated_at: Optional[str] = None
    fetched_at: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPO_HREF = re.compile(r"^/[\w.\-]+/[\w.\-]+$")
_TOPIC_HREF = re.compile(r"^/topics/")
_STARS_HREF = re.compile(r"/stargazers")


def _parse_stars(text: str) -> Optional[int]:
    """Convert '1,234' or '1.2k' to an integer."""
    text = text.strip().replace(",", "").replace(" ", "").lower()
    if not text:
        return None
    if text.endswith("k"):
        try:
            return int(float(text[:-1]) * 1_000)
        except ValueError:
            return None
    try:
        return int(text)
    except ValueError:
        return None


def _pick_description(article: BeautifulSoup) -> Optional[str]:
    """Return the first <p> that is not a login-wall message."""
    for p in article.find_all("p"):
        text = p.get_text(strip=True)
        if text and "sign in" not in text.lower():
            return text
    return None


def fetch_github_explore() -> list[GitHubProject]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        response = client.get("https://github.com/explore", headers=headers)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    fetched_at = datetime.now(timezone.utc).isoformat()
    projects: list[GitHubProject] = []
    seen: set[str] = set()

    articles = soup.find_all("article")

    for article in articles:
        heading = article.find(["h2", "h3"])
        if not heading:
            continue

        repo_links = heading.find_all("a", href=_REPO_HREF)
        if not repo_links:
            continue

        href: str = repo_links[-1]["href"]
        if href in seen:
            continue
        seen.add(href)

        parts = href.strip("/").split("/")
        if len(parts) != 2:
            continue
        owner, name = parts

        topics = [
            a.get_text(strip=True) for a in article.find_all("a", href=_TOPIC_HREF)
        ]

        lang_el = article.find("span", itemprop="programmingLanguage")
        if not lang_el:
            for span in article.find_all("span"):
                cls = " ".join(span.get("class") or [])
                if "language" in cls.lower():
                    lang_el = span
                    break
        language = lang_el.get_text(strip=True) if lang_el else None

        stars: Optional[int] = None
        for a in article.find_all("a", href=_STARS_HREF):
            stars = _parse_stars(a.get_text(strip=True))
            break

        time_el = article.find("relative-time") or article.find("time")
        updated_at: Optional[str] = None
        if time_el:
            updated_at = time_el.get("datetime") or time_el.get_text(strip=True)

        projects.append(
            GitHubProject(
                owner=owner,
                name=name,
                full_name=f"{owner}/{name}",
                url=f"https://github.com{href}",
                description=_pick_description(article),
                language=language,
                topics=topics,
                stars=stars,
                updated_at=updated_at,
                fetched_at=fetched_at,
            )
        )

    if not projects:
        for heading in soup.find_all(["h2", "h3"]):
            repo_links = heading.find_all("a", href=_REPO_HREF)
            if not repo_links:
                continue
            href = repo_links[-1]["href"]
            if href in seen:
                continue
            seen.add(href)
            parts = href.strip("/").split("/")
            if len(parts) != 2:
                continue
            owner, name = parts
            projects.append(
                GitHubProject(
                    owner=owner,
                    name=name,
                    full_name=f"{owner}/{name}",
                    url=f"https://github.com{href}",
                    fetched_at=fetched_at,
                )
            )

    return projects


def main() -> None:
    projects = fetch_github_explore()
    output = [p.model_dump() for p in projects]

    with open("projects.json", "w") as f:
        json.dump(output, f)

    print(f"\nFetched {len(output)} projects from github.com/explore", flush=True)
    for p in projects:
        stars_str = f" ⭐ {p.stars:,}" if p.stars is not None else ""
        lang_str = f"[{p.language}]" if p.language else ""
        desc_str = f" — {p.description}" if p.description else ""
        print(f"  • {p.full_name}{stars_str} {lang_str}{desc_str}", flush=True)


if __name__ == "__main__":
    main()

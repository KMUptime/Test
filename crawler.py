#!/usr/bin/env python3
"""
Reddit AITA Crawler
Fetches posts from r/AmItheAsshole using Reddit's public JSON API.
"""

import json
import csv
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path

import requests

BASE_URL = "https://www.reddit.com/r/AmItheAsshole"
HEADERS = {"User-Agent": "aita-crawler/1.0 (research)"}
REQUEST_DELAY = 2  # seconds between requests (Reddit rate limit: ~1 req/sec)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def fetch_posts(sort: str = "new", limit: int = 100, max_posts: int = 1000) -> list[dict]:
    """
    Fetch posts from r/AmItheAsshole.

    Args:
        sort: Listing type — 'new', 'hot', 'top', 'rising'
        limit: Posts per request (max 100)
        max_posts: Total posts to collect

    Returns:
        List of post dicts
    """
    url = f"{BASE_URL}/{sort}.json"
    posts = []
    after = None
    limit = min(limit, 100)

    while len(posts) < max_posts:
        params = {"limit": limit, "raw_json": 1}
        if after:
            params["after"] = after

        log.info("Fetching %s posts (collected %d so far, after=%s)", sort, len(posts), after)

        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error("Request failed: %s", e)
            break

        data = resp.json()
        children = data.get("data", {}).get("children", [])
        if not children:
            log.info("No more posts available.")
            break

        for child in children:
            post = parse_post(child["data"])
            if post:
                posts.append(post)

        after = data.get("data", {}).get("after")
        if not after:
            log.info("Reached end of listing.")
            break

        time.sleep(REQUEST_DELAY)

    return posts[:max_posts]


def parse_post(raw: dict) -> dict | None:
    """Extract relevant fields from a raw post object."""
    # Skip stickied mod posts
    if raw.get("stickied") or raw.get("distinguished") == "moderator":
        return None

    created_utc = raw.get("created_utc", 0)
    return {
        "id": raw.get("id"),
        "title": raw.get("title", ""),
        "selftext": raw.get("selftext", ""),
        "author": raw.get("author", "[deleted]"),
        "score": raw.get("score", 0),
        "upvote_ratio": raw.get("upvote_ratio", 0.0),
        "num_comments": raw.get("num_comments", 0),
        "flair": raw.get("link_flair_text", ""),
        "url": f"https://www.reddit.com{raw.get('permalink', '')}",
        "created_utc": int(created_utc),
        "created_date": datetime.utcfromtimestamp(created_utc).strftime("%Y-%m-%d %H:%M:%S"),
        "is_self": raw.get("is_self", True),
        "over_18": raw.get("over_18", False),
    }


def save_json(posts: list[dict], path: str) -> None:
    Path(path).write_text(json.dumps(posts, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Saved %d posts to %s", len(posts), path)


def save_csv(posts: list[dict], path: str) -> None:
    if not posts:
        log.warning("No posts to save.")
        return
    fieldnames = list(posts[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(posts)
    log.info("Saved %d posts to %s", len(posts), path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Crawl r/AmItheAsshole posts")
    parser.add_argument("--sort", default="new", choices=["new", "hot", "top", "rising"],
                        help="Listing sort order (default: new)")
    parser.add_argument("--max-posts", type=int, default=500,
                        help="Maximum number of posts to collect (default: 500)")
    parser.add_argument("--output", default="aita_posts",
                        help="Output filename without extension (default: aita_posts)")
    parser.add_argument("--format", default="json", choices=["json", "csv", "both"],
                        help="Output format (default: json)")
    args = parser.parse_args()

    log.info("Starting AITA crawler: sort=%s, max_posts=%d", args.sort, args.max_posts)
    posts = fetch_posts(sort=args.sort, max_posts=args.max_posts)
    log.info("Collected %d posts total.", len(posts))

    if not posts:
        log.warning("No posts collected. Exiting.")
        return

    if args.format in ("json", "both"):
        save_json(posts, f"{args.output}.json")
    if args.format in ("csv", "both"):
        save_csv(posts, f"{args.output}.csv")


if __name__ == "__main__":
    main()

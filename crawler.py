#!/usr/bin/env python3
"""
Reddit AITA Crawler
Fetches posts from r/AmItheAsshole using Reddit's public JSON API.

Two-phase crawl:
  Phase 1: Collect posts (fast — 100 posts per request)
  Phase 2: Enrich each post with its top comment (one request per post)

Checkpointing is used throughout so the crawl can be safely interrupted
and resumed without losing progress.
"""

import json
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path

import requests

BASE_URL = "https://www.reddit.com/r/AmItheAsshole"
HEADERS = {"User-Agent": "aita-crawler/1.0 (research)"}
REQUEST_DELAY = 1.1  # seconds — Reddit public API: ~60 req/min

VALID_FLAIRS = {"NTA", "YTA", "ESH", "NAH", "INFO"}
MIN_SCORE = 50
MIN_COMMENTS = 10
CHECKPOINT_EVERY = 500  # write checkpoint after every N accepted posts

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Post crawl (Phase 1)
# ---------------------------------------------------------------------------

def fetch_posts(
    sort: str,
    max_posts: int,
    checkpoint_path: Path,
) -> list[dict]:
    """
    Collect up to *max_posts* filtered posts from r/AITA.

    Resumes from *checkpoint_path* if it exists.
    """
    posts, after = _load_checkpoint(checkpoint_path)
    log.info("Resuming from checkpoint: %d posts already collected.", len(posts))

    url = f"{BASE_URL}/{sort}.json"

    while len(posts) < max_posts:
        params = {"limit": 100, "raw_json": 1}
        if after:
            params["after"] = after

        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error("Request failed: %s — saving checkpoint and exiting.", e)
            _save_checkpoint(checkpoint_path, posts, after)
            break

        data = resp.json().get("data", {})
        children = data.get("children", [])
        if not children:
            log.info("No more posts returned by Reddit.")
            break

        accepted = 0
        for child in children:
            post = _parse_post(child["data"])
            if post:
                posts.append(post)
                accepted += 1

        after = data.get("after")
        log.info(
            "Page fetched — accepted %d/%d | total %d/%d | after=%s",
            accepted, len(children), len(posts), max_posts, after,
        )

        if len(posts) % CHECKPOINT_EVERY < accepted:
            _save_checkpoint(checkpoint_path, posts, after)

        if not after:
            log.info("Reached end of listing.")
            break

        time.sleep(REQUEST_DELAY)

    posts = posts[:max_posts]
    _save_checkpoint(checkpoint_path, posts, after)
    return posts


def _parse_post(raw: dict) -> dict | None:
    """Return a cleaned post dict, or None if the post should be skipped."""
    # Must be a text post with actual content
    if not raw.get("is_self"):
        return None
    selftext = raw.get("selftext", "")
    if selftext in ("[deleted]", "[removed]", ""):
        return None

    # Skip mod/stickied posts
    if raw.get("stickied") or raw.get("distinguished") == "moderator":
        return None

    # Skip edited posts — edited content no longer matches the verdict the
    # community voted on
    if raw.get("edited") is not False:
        return None

    # Must have a recognised verdict flair
    flair = (raw.get("link_flair_text") or "").strip()
    if flair not in VALID_FLAIRS:
        return None

    # Minimum engagement thresholds
    if raw.get("score", 0) < MIN_SCORE:
        return None
    if raw.get("num_comments", 0) < MIN_COMMENTS:
        return None

    score = raw.get("score", 0)
    upvote_ratio = raw.get("upvote_ratio") or 0.0
    # Reddit fuzzes raw counts; these are approximations
    upvotes = round(score / upvote_ratio) if upvote_ratio > 0 else score
    downvotes = max(0, upvotes - score)

    created_utc = int(raw.get("created_utc", 0))
    return {
        "id": raw.get("id"),
        "title": raw.get("title", ""),
        "selftext": selftext,
        "verdict": flair,
        "score": score,
        "upvote_ratio": upvote_ratio,
        "upvotes": upvotes,
        "downvotes": downvotes,
        "num_comments": raw.get("num_comments", 0),
        "num_awards": raw.get("total_awards_received", 0),
        "created_utc": created_utc,
        "created_date": datetime.utcfromtimestamp(created_utc).strftime("%Y-%m-%d %H:%M:%S"),
        # Placeholder — populated in Phase 2
        "top_comment_body": None,
        "top_comment_score": None,
    }


# ---------------------------------------------------------------------------
# Comment enrichment (Phase 2)
# ---------------------------------------------------------------------------

def enrich_with_comments(posts: list[dict], checkpoint_path: Path) -> None:
    """
    Fetch the top non-stickied comment for each post in-place.

    Skips posts that already have a comment populated (safe to resume).
    Saves a checkpoint every CHECKPOINT_EVERY enriched posts.
    """
    pending = [p for p in posts if p["top_comment_body"] is None]
    log.info(
        "Phase 2: fetching top comments for %d posts (~%.1f hours at 1 req/s).",
        len(pending), len(pending) / 3600,
    )

    enriched = 0
    for post in pending:
        body, score = _fetch_top_comment(post["id"])
        post["top_comment_body"] = body
        post["top_comment_score"] = score
        enriched += 1

        if enriched % CHECKPOINT_EVERY == 0:
            log.info("Comment progress: %d/%d", enriched, len(pending))
            _save_checkpoint(checkpoint_path, posts, after=None)

        time.sleep(REQUEST_DELAY)

    _save_checkpoint(checkpoint_path, posts, after=None)
    log.info("Phase 2 complete.")


def _fetch_top_comment(post_id: str) -> tuple[str, int]:
    """Return (body, score) of the top non-stickied comment, or ('', 0)."""
    url = f"https://www.reddit.com/r/AmItheAsshole/comments/{post_id}.json"
    params = {"sort": "top", "limit": 5, "depth": 1, "raw_json": 1}
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
        resp.raise_for_status()
        comments = resp.json()[1]["data"]["children"]
        for child in comments:
            if child.get("kind") != "t1":
                continue
            d = child["data"]
            if d.get("stickied") or d.get("distinguished") == "moderator":
                continue
            body = d.get("body", "")
            if body in ("[deleted]", "[removed]", ""):
                continue
            return body, d.get("score", 0)
    except Exception as e:
        log.warning("Failed to fetch comment for post %s: %s", post_id, e)
    return "", 0


# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------

def _save_checkpoint(path: Path, posts: list[dict], after: str | None) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({"after": after, "posts": posts}, ensure_ascii=False),
        encoding="utf-8",
    )
    tmp.replace(path)  # atomic replace
    log.debug("Checkpoint saved: %d posts, after=%s", len(posts), after)


def _load_checkpoint(path: Path) -> tuple[list[dict], str | None]:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("posts", []), data.get("after")
    return [], None


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_json(posts: list[dict], path: str) -> None:
    Path(path).write_text(
        json.dumps(posts, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info("Saved %d posts → %s", len(posts), path)


def save_csv(posts: list[dict], path: str) -> None:
    if not posts:
        log.warning("No posts to save.")
        return
    import csv
    fieldnames = list(posts[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(posts)
    log.info("Saved %d posts → %s", len(posts), path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl r/AmItheAsshole posts with verdict flairs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--sort", default="top",
                        choices=["new", "hot", "top", "rising"],
                        help="Listing sort order")
    parser.add_argument("--max-posts", type=int, default=50_000,
                        help="Target number of filtered posts to collect")
    parser.add_argument("--output", default="aita_posts",
                        help="Output filename (no extension)")
    parser.add_argument("--format", default="json",
                        choices=["json", "csv", "both"],
                        help="Output format")
    parser.add_argument("--skip-comments", action="store_true",
                        help="Skip Phase 2 comment fetching (faster, incomplete data)")
    parser.add_argument("--checkpoint", default="aita_checkpoint.json",
                        help="Checkpoint file for resuming interrupted crawls")
    args = parser.parse_args()

    checkpoint_path = Path(args.checkpoint)

    # Phase 1 — collect posts
    log.info("Phase 1: collecting posts (sort=%s, target=%d)", args.sort, args.max_posts)
    posts = fetch_posts(args.sort, args.max_posts, checkpoint_path)
    log.info("Phase 1 complete: %d posts collected.", len(posts))

    # Phase 2 — enrich with top comments
    if not args.skip_comments:
        enrich_with_comments(posts, checkpoint_path)

    if not posts:
        log.warning("No posts collected.")
        return

    if args.format in ("json", "both"):
        save_json(posts, f"{args.output}.json")
    if args.format in ("csv", "both"):
        save_csv(posts, f"{args.output}.csv")


if __name__ == "__main__":
    main()

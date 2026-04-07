#!/usr/bin/env python3
"""
AITA Crawler Worker
Crawls a specific time window of r/AmItheAsshole using Reddit's search API
with epoch timestamp pagination, bypassing the ~1000-post listing cap.

Intended to be launched by orchestrate.py but can also run standalone.
"""

import json
import random
import time
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

# Re-use shared helpers from crawler.py
from crawler import (
    HEADERS,
    VALID_FLAIRS,
    MIN_SCORE,
    MIN_COMMENTS,
    _parse_post,
    _fetch_top_comment,
    save_json,
    save_csv,
)

SEARCH_URL = "https://www.reddit.com/r/AmItheAsshole/search.json"
# Broad flair filter — _parse_post is the authoritative gate
FLAIR_QUERY = "flair:NTA OR flair:YTA OR flair:ESH OR flair:NAH OR flair:INFO"

REQUEST_DELAY = 1.1      # base seconds between requests
JITTER = 0.3             # random seconds added to base delay
MAX_RETRIES = 5          # max retries on 429 / network error
CHECKPOINT_EVERY = 500   # save checkpoint after every N accepted posts

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase 1 — windowed post crawl
# ---------------------------------------------------------------------------

def fetch_posts_windowed(
    window_start: int,
    window_end: int,
    quota: int,
    checkpoint_path: Path,
) -> list[dict]:
    """
    Collect up to *quota* filtered posts within [window_start, window_end] (epoch).

    Paginates backwards in time using `before` timestamps.
    Resumes from *checkpoint_path* if it exists.
    """
    posts, current_before = _load_checkpoint(checkpoint_path)
    if current_before is None:
        current_before = window_end

    log.info(
        "Worker starting: window %s → %s | collected %d | quota %d",
        _fmt_ts(window_start), _fmt_ts(window_end), len(posts), quota,
    )

    while len(posts) < quota and current_before > window_start:
        params = {
            "q": FLAIR_QUERY,
            "restrict_sr": 1,
            "sort": "new",
            "t": "all",
            "limit": 100,
            "before": current_before,
            "after": window_start,
            "raw_json": 1,
        }

        data = _request(SEARCH_URL, params)
        if data is None:
            log.warning("Request failed — saving checkpoint and stopping.")
            break

        children = data.get("data", {}).get("children", [])
        if not children:
            log.info("No more results in this window.")
            break

        accepted = 0
        oldest_ts = current_before
        for child in children:
            raw = child["data"]
            created = int(raw.get("created_utc", 0))
            # Guard: only accept posts strictly within our window
            if not (window_start <= created <= window_end):
                continue
            post = _parse_post(raw)
            if post:
                posts.append(post)
                accepted += 1
            oldest_ts = min(oldest_ts, created)

        current_before = oldest_ts - 1

        log.info(
            "Page: accepted %d/%d | total %d/%d | next before %s",
            accepted, len(children), len(posts), quota, _fmt_ts(current_before),
        )

        if len(posts) % CHECKPOINT_EVERY < accepted or accepted == 0:
            _save_checkpoint(checkpoint_path, posts, current_before)

        _sleep()

    posts = posts[:quota]
    _save_checkpoint(checkpoint_path, posts, current_before)
    return posts


# ---------------------------------------------------------------------------
# Phase 2 — comment enrichment
# ---------------------------------------------------------------------------

def enrich_with_comments(posts: list[dict], checkpoint_path: Path) -> None:
    """Fetch top comment for each post in-place. Skips already-enriched posts."""
    pending = [p for p in posts if p.get("top_comment_body") is None]
    log.info(
        "Phase 2: %d comments to fetch (~%.1f hours at 1 req/s).",
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
            _save_checkpoint(checkpoint_path, posts, current_before=None)

        _sleep()

    _save_checkpoint(checkpoint_path, posts, current_before=None)
    log.info("Phase 2 complete.")


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _request(url: str, params: dict) -> dict | None:
    """GET with exponential backoff on 429 / transient errors."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if resp.status_code == 429:
                wait = 5 * (2 ** attempt)
                log.warning("429 rate-limited — backing off %ds (attempt %d/%d)", wait, attempt + 1, MAX_RETRIES)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            wait = 5 * (2 ** attempt)
            log.warning("Request error: %s — retrying in %ds", e, wait)
            time.sleep(wait)
    return None


def _sleep() -> None:
    time.sleep(REQUEST_DELAY + random.uniform(0, JITTER))


# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------

def _save_checkpoint(path: Path, posts: list[dict], current_before: int | None) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({"current_before": current_before, "posts": posts}, ensure_ascii=False),
        encoding="utf-8",
    )
    tmp.replace(path)


def _load_checkpoint(path: Path) -> tuple[list[dict], int | None]:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("posts", []), data.get("current_before")
    return [], None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _fmt_ts(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")


def _now_epoch() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


# ---------------------------------------------------------------------------
# Entry point (standalone use)
# ---------------------------------------------------------------------------

def main(args=None) -> None:
    parser = argparse.ArgumentParser(
        description="AITA crawler worker — crawls a specific time window.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--worker-id", type=int, default=0)
    parser.add_argument("--window-start", type=int, required=True,
                        help="Window start as Unix epoch")
    parser.add_argument("--window-end", type=int, default=None,
                        help="Window end as Unix epoch (default: now)")
    parser.add_argument("--quota", type=int, default=6500,
                        help="Maximum posts to collect in this window")
    parser.add_argument("--output-dir", default="runs",
                        help="Directory for checkpoint and output files")
    parser.add_argument("--phase", default="both", choices=["1", "2", "both"],
                        help="Which phase(s) to run")
    parser.add_argument("--format", default="json", choices=["json", "csv", "both"])
    args = parser.parse_args(args)

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [W{args.worker_id}] %(levelname)s %(message)s",
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_path = output_dir / f"worker_{args.worker_id}_checkpoint.json"
    output_stem = str(output_dir / f"worker_{args.worker_id}_posts")

    window_end = args.window_end or _now_epoch()

    posts = []

    if args.phase in ("1", "both"):
        posts = fetch_posts_windowed(
            window_start=args.window_start,
            window_end=window_end,
            quota=args.quota,
            checkpoint_path=checkpoint_path,
        )
        log.info("Phase 1 complete: %d posts collected.", len(posts))
    else:
        # Phase 2 only — load from checkpoint
        posts, _ = _load_checkpoint(checkpoint_path)
        log.info("Loaded %d posts from checkpoint for Phase 2.", len(posts))

    if args.phase in ("2", "both") and posts:
        enrich_with_comments(posts, checkpoint_path)

    if args.format in ("json", "both"):
        save_json(posts, f"{output_stem}.json")
    if args.format in ("csv", "both"):
        save_csv(posts, f"{output_stem}.csv")


if __name__ == "__main__":
    main()

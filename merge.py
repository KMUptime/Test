#!/usr/bin/env python3
"""
AITA Crawler Merger
Combines per-worker output files, deduplicates by post ID,
and prints a summary of the merged dataset.
"""

import argparse
import csv
import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


def merge(output_dir: Path, merged_output: Path, fmt: str = "json") -> list[dict]:
    """
    Load all worker output files from *output_dir*, deduplicate, sort, and save.

    Deduplication rule: if the same post ID appears in two workers, keep the
    copy that has top_comment_body populated; otherwise keep either.

    Returns the merged post list.
    """
    worker_files = sorted(output_dir.glob("worker_*_posts.json"))
    if not worker_files:
        log.error("No worker output files found in %s", output_dir)
        return []

    seen: dict[str, dict] = {}
    per_worker_counts: dict[str, int] = {}

    for path in worker_files:
        posts = json.loads(path.read_text(encoding="utf-8"))
        per_worker_counts[path.name] = len(posts)
        for post in posts:
            pid = post["id"]
            if pid not in seen:
                seen[pid] = post
            else:
                # Prefer the copy with a top comment
                existing_has_comment = bool(seen[pid].get("top_comment_body"))
                incoming_has_comment = bool(post.get("top_comment_body"))
                if incoming_has_comment and not existing_has_comment:
                    seen[pid] = post

    merged = sorted(seen.values(), key=lambda p: p["created_utc"], reverse=True)
    duplicates = sum(per_worker_counts.values()) - len(merged)

    _print_summary(merged, per_worker_counts, duplicates)
    _save(merged, merged_output, fmt)

    return merged


def _print_summary(posts: list[dict], per_worker: dict[str, int], duplicates: int) -> None:
    print("\n" + "=" * 60)
    print("MERGE SUMMARY")
    print("=" * 60)

    print("\nPer-worker post counts:")
    for name, count in per_worker.items():
        print(f"  {name}: {count:,}")

    print(f"\nTotal before dedup : {sum(per_worker.values()):,}")
    print(f"Duplicates removed : {duplicates:,}")
    print(f"Final post count   : {len(posts):,}")

    if not posts:
        return

    verdict_counts = Counter(p["verdict"] for p in posts)
    print("\nVerdict distribution:")
    for verdict, count in sorted(verdict_counts.items(), key=lambda x: -x[1]):
        pct = count / len(posts) * 100
        print(f"  {verdict:<6} {count:>6,}  ({pct:.1f}%)")

    has_comment = sum(1 for p in posts if p.get("top_comment_body"))
    print(f"\nPosts with top comment : {has_comment:,} / {len(posts):,}")

    dates = [p["created_utc"] for p in posts]
    fmt = lambda ts: datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    print(f"Date range             : {fmt(min(dates))} → {fmt(max(dates))}")
    print("=" * 60 + "\n")


def _save(posts: list[dict], path: Path, fmt: str) -> None:
    if fmt in ("json", "both"):
        path.write_text(json.dumps(posts, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("Saved %d posts → %s", len(posts), path)

    if fmt in ("csv", "both"):
        csv_path = path.with_suffix(".csv")
        if posts:
            fieldnames = list(posts[0].keys())
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(posts)
            log.info("Saved %d posts → %s", len(posts), csv_path)


# ---------------------------------------------------------------------------
# Entry point (standalone use)
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge per-worker AITA crawler outputs into a single dataset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input-dir", default="runs",
                        help="Directory containing worker_*_posts.json files")
    parser.add_argument("--output", default="aita_posts_merged.json",
                        help="Path for merged output file")
    parser.add_argument("--format", default="json", choices=["json", "csv", "both"])
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    merge(
        output_dir=Path(args.input_dir),
        merged_output=Path(args.output),
        fmt=args.format,
    )


if __name__ == "__main__":
    main()

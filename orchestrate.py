#!/usr/bin/env python3
"""
AITA Crawler Orchestrator
Launches N parallel worker processes, each crawling a distinct time window,
then merges the results when all workers finish.

Time windows are weighted by post density: 2018–2024 gets narrower slices
than the sparse early years so each worker collects a similar post count.
"""

import argparse
import json
import logging
import multiprocessing as mp
import time
from datetime import datetime, timezone
from pathlib import Path

import merge
import worker as worker_module

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical 8-window partition (epoch timestamps)
# Adjust end of last window to runtime via _now_epoch()
# ---------------------------------------------------------------------------

def _dt(year: int, month: int, day: int = 1) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp())

def _now_epoch() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())

CANONICAL_WINDOWS = [
    (_dt(2013, 6),  _dt(2016, 12, 31)),  # W0 — sparse early years
    (_dt(2017, 1),  _dt(2018, 6,  30)),  # W1
    (_dt(2018, 7),  _dt(2019, 6,  30)),  # W2 — growth phase
    (_dt(2019, 7),  _dt(2020, 6,  30)),  # W3 — high volume
    (_dt(2020, 7),  _dt(2021, 6,  30)),  # W4 — high volume
    (_dt(2021, 7),  _dt(2022, 12, 31)),  # W5
    (_dt(2023, 1),  _dt(2024, 6,  30)),  # W6
    (_dt(2024, 7),  None),               # W7 — present (end set at runtime)
]


def build_windows(n_workers: int) -> list[tuple[int, int]]:
    """
    Collapse the 8 canonical windows into exactly n_workers windows by
    merging adjacent windows when n_workers < 8, or splitting the densest
    windows when n_workers > 8 (capped at 8 for simplicity).
    """
    now = _now_epoch()
    windows = [(s, e if e is not None else now) for s, e in CANONICAL_WINDOWS]

    if n_workers >= len(windows):
        return windows

    # Merge adjacent windows until we have n_workers
    while len(windows) > n_workers:
        # Merge the two adjacent windows with the smallest combined span
        spans = [(windows[i][1] - windows[i][0]) + (windows[i+1][1] - windows[i+1][0])
                 for i in range(len(windows) - 1)]
        idx = spans.index(min(spans))
        merged = (windows[idx][0], windows[idx + 1][1])
        windows = windows[:idx] + [merged] + windows[idx + 2:]

    return windows


# ---------------------------------------------------------------------------
# Worker process target
# ---------------------------------------------------------------------------

def _run_worker(worker_id: int, window_start: int, window_end: int,
                quota: int, output_dir: str, phase: str) -> None:
    """Called inside a subprocess — delegates to worker.main()."""
    worker_module.main([
        "--worker-id", str(worker_id),
        "--window-start", str(window_start),
        "--window-end", str(window_end),
        "--quota", str(quota),
        "--output-dir", output_dir,
        "--phase", phase,
    ])


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def orchestrate(
    n_workers: int,
    target: int,
    output_dir: Path,
    phase: str,
    resume: bool,
) -> None:
    windows = build_windows(n_workers)
    n_workers = len(windows)  # may be less than requested if history is short
    quota_each = target // n_workers
    quota_remainder = target % n_workers

    log.info("Launching %d workers | target %d posts | phase=%s", n_workers, target, phase)
    for i, (ws, we) in enumerate(windows):
        log.info("  W%d: %s → %s | quota %d", i, _fmt(ws), _fmt(we),
                 quota_each + (quota_remainder if i == 0 else 0))

    processes: list[mp.Process] = []
    for i, (ws, we) in enumerate(windows):
        quota = quota_each + (quota_remainder if i == 0 else 0)

        # Skip workers whose output already exists (resume mode)
        output_file = output_dir / f"worker_{i}_posts.json"
        if resume and output_file.exists():
            log.info("W%d: output exists, skipping (--resume).", i)
            continue

        p = mp.Process(
            target=_run_worker,
            args=(i, ws, we, quota, str(output_dir), phase),
            name=f"worker-{i}",
            daemon=True,
        )
        p.start()
        processes.append(p)

    if not processes:
        log.info("All workers already complete (--resume).")
    else:
        _monitor(processes, output_dir)

    if phase in ("both", "1"):
        log.info("All workers done. Merging results...")
        merge.merge(output_dir, output_dir / "aita_posts_merged.json")


def _monitor(processes: list[mp.Process], output_dir: Path) -> None:
    """Poll worker liveness every 60s, logging checkpoint progress."""
    poll_interval = 60
    while any(p.is_alive() for p in processes):
        time.sleep(poll_interval)
        for p in processes:
            wid = int(p.name.split("-")[1])
            status = "running" if p.is_alive() else f"done (exit {p.exitcode})"
            count = _checkpoint_count(output_dir, wid)
            log.info("  %s: %s | posts collected: %d", p.name, status, count)

    # Final exit code check
    failed = [p for p in processes if p.exitcode != 0]
    if failed:
        log.warning("Workers exited with errors: %s", [p.name for p in failed])
    else:
        log.info("All workers completed successfully.")


def _checkpoint_count(output_dir: Path, worker_id: int) -> int:
    cp = output_dir / f"worker_{worker_id}_checkpoint.json"
    if not cp.exists():
        return 0
    try:
        data = json.loads(cp.read_text(encoding="utf-8"))
        return len(data.get("posts", []))
    except Exception:
        return 0


def _fmt(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orchestrate parallel AITA crawler workers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of parallel worker processes")
    parser.add_argument("--target", type=int, default=50_000,
                        help="Total filtered posts to collect across all workers")
    parser.add_argument("--output-dir", default="runs",
                        help="Directory for worker checkpoints and output files")
    parser.add_argument("--phase", default="both", choices=["1", "2", "both"],
                        help="Phase 1=posts only, 2=comments only, both=full run")
    parser.add_argument("--resume", action="store_true",
                        help="Skip workers whose output file already exists")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [orchestrator] %(levelname)s %(message)s",
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    orchestrate(
        n_workers=args.workers,
        target=args.target,
        output_dir=output_dir,
        phase=args.phase,
        resume=args.resume,
    )


if __name__ == "__main__":
    main()

"""Structured logging configuration for the pipeline."""

import logging
import sys
import time
from pathlib import Path

import config


def setup_logging() -> None:
    """Configure logging with stdout + file handlers."""
    log_path = Path(config.LOG_FILE)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    fmt = "%(asctime)s | %(levelname)-7s | %(name)-25s | %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(str(log_path), encoding="utf-8"),
        ],
    )
    # Quiet noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)


class ProgressTracker:
    """Simple progress tracker with ETA calculation."""

    def __init__(self, total: int, stage_name: str):
        self.total = total
        self.stage_name = stage_name
        self.processed = 0
        self.start_time = time.time()
        self.logger = logging.getLogger(stage_name)

    def tick(self, company_name: str, result: str) -> None:
        self.processed += 1
        elapsed = time.time() - self.start_time
        rate = self.processed / elapsed if elapsed > 0 else 0
        remaining = (self.total - self.processed) / rate if rate > 0 else 0

        eta_min = int(remaining // 60)
        eta_sec = int(remaining % 60)

        self.logger.info(
            "[%d/%d] %s -> %s (ETA: %dm%02ds)",
            self.processed, self.total, company_name, result, eta_min, eta_sec,
        )

    def summary(self, results: dict[str, int]) -> None:
        elapsed = time.time() - self.start_time
        parts = ", ".join(f"{k}: {v}" for k, v in results.items())
        self.logger.info(
            "%s complete in %.1fs: %s", self.stage_name, elapsed, parts
        )

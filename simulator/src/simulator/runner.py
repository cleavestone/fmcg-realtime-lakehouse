"""Continuous mode: pick a weighted event, run it as one transaction, wait, repeat.

Shutdown is cooperative: SIGTERM/SIGINT only set a flag, which the loop checks between
events, so a transaction in flight always commits or rolls back before the process exits.
"""

import logging
import signal
import threading
import time
from collections import Counter
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import psycopg

from simulator import db, rules
from simulator.events import EVENTS, Context, Skip, build_context, refresh_popularity

log = logging.getLogger("simulator.run")


def _fmt(detail: dict[str, Any]) -> str:
    return " ".join(f"{k}={v}" for k, v in detail.items())


class Stats:
    """Per-window counters for the periodic summary line."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.started = time.monotonic()
        self.ok: Counter[str] = Counter()
        self.skipped: Counter[str] = Counter()
        self.failed: Counter[str] = Counter()

    def summary(self) -> str:
        elapsed = max(time.monotonic() - self.started, 1e-9)
        total = sum(self.ok.values())
        by_type = " ".join(
            f"{name}={self.ok[name] / elapsed:.2f}/s" for name in EVENTS if self.ok[name]
        )
        return (
            f"summary window_s={elapsed:.0f} events={total} rate={total / elapsed:.2f}/s "
            f"skipped={sum(self.skipped.values())} failed={sum(self.failed.values())} {by_type}"
        )


class Runner:
    def __init__(self, cfg: dict[str, Any], rate: float, duration: float | None, seed: int) -> None:
        self.cfg = cfg
        self.rate = rate
        self.duration = duration
        self.seed = seed
        self.stop = threading.Event()
        self.stats = Stats()
        self.tz = timezone(timedelta(hours=cfg["timezone_offset_hours"]))
        weights = cfg["event_weights"]
        unknown = set(weights) - set(EVENTS)
        if unknown:
            raise ValueError(f"config event_weights has unknown events: {sorted(unknown)}")
        self.weights = {name: w for name, w in weights.items() if w > 0}

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        log.info("received %s; finishing current event and stopping", signal.Signals(signum).name)
        self.stop.set()

    def _rate_now(self) -> float:
        tod = self.cfg["time_of_day"]
        if not tod["enabled"]:
            return self.rate
        return self.rate * tod["multipliers"][datetime.now(UTC).astimezone(self.tz).hour]

    def run_one(self, conn: psycopg.Connection, ctx: Context, name: str) -> None:
        started = time.perf_counter()
        try:
            with conn.transaction():
                result = EVENTS[name](conn, ctx)
        except Skip as e:
            self.stats.skipped[name] += 1
            log.info("event=%s status=skipped reason=%r", name, str(e))
            return
        except (
            psycopg.errors.IntegrityError,
            psycopg.errors.DeadlockDetected,
            psycopg.errors.LockNotAvailable,
        ) as e:
            # Rolled back; the business carries on. Connection-level errors propagate instead.
            self.stats.failed[name] += 1
            log.warning("event=%s status=failed error=%r", name, str(e).strip())
            return
        latency_ms = (time.perf_counter() - started) * 1000
        self.stats.ok[name] += 1
        log.info(
            "event=%s status=ok entity_id=%s latency_ms=%.1f %s",
            name,
            result.entity_id,
            latency_ms,
            _fmt(result.detail),
        )

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        cfg = self.cfg
        with db.connect() as conn:
            conn.autocommit = True  # each event opens its own explicit transaction
            ctx = build_context(conn, cfg, self.seed)
            log.info(
                "simulator started rate=%s/s duration=%s seed=%s time_of_day=%s",
                self.rate,
                self.duration or "unlimited",
                self.seed,
                cfg["time_of_day"]["enabled"],
            )
            started = time.monotonic()
            next_summary = started + cfg["summary_interval_seconds"]
            next_popularity = started + cfg["popularity_refresh_seconds"]

            while not self.stop.is_set():
                self.run_one(conn, ctx, rules.weighted_choice(ctx.rng, self.weights))

                now = time.monotonic()
                if now >= next_summary:
                    log.info(self.stats.summary())
                    self.stats.reset()
                    next_summary = now + cfg["summary_interval_seconds"]
                if now >= next_popularity:
                    refresh_popularity(conn, ctx)
                    next_popularity = now + cfg["popularity_refresh_seconds"]
                if self.duration and now - started >= self.duration:
                    log.info("duration reached")
                    break

                rate = self._rate_now()
                self.stop.wait(ctx.rng.expovariate(rate) if rate > 0 else 1.0)

            log.info(self.stats.summary())
        log.info("simulator stopped cleanly")

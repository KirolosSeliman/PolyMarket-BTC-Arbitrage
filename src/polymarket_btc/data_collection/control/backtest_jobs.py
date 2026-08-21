"""Background job runner for `backtest_engine.run_backtest`: the same
async-task-plus-pollable-status shape `CollectionRunManager` already uses
for collection runs (`runs.py`'s `RunState`/`start()`/`status()`), adapted
for a sync, CPU-bound function that must not block the event loop -- run via
`loop.run_in_executor` rather than awaited directly. A backtest can take a
while (long date ranges, wide parameter sweeps), so the server hands back a
job id immediately and the page polls, exactly like `/api/collect/status`
already does for a live collection run.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import time
from typing import Any
import uuid

from .backtest_engine import run_backtest


@dataclass(slots=True)
class BacktestJob:
    job_id: str
    started_at_ns: int
    task: asyncio.Task | None = None
    ended_at_ns: int | None = None
    result: dict[str, object] | None = None
    error: str | None = None
    # 0..1, updated from run_backtest's own on_progress callback -- called
    # from the executor thread run_backtest actually runs in (see start()
    # below), not the event-loop thread, but a plain float attribute set is
    # safe to read cross-thread without a lock (GIL-atomic, and a stale read
    # is at worst one step behind, never torn).
    progress: float = 0.0


@dataclass(slots=True)
class BacktestJobManager:
    jobs: dict[str, BacktestJob] = field(default_factory=dict)

    def start(self, **kwargs: Any) -> BacktestJob:
        job_id = uuid.uuid4().hex
        job = BacktestJob(job_id=job_id, started_at_ns=time.time_ns())
        loop = asyncio.get_event_loop()

        def on_progress(fraction: float) -> None:
            job.progress = fraction

        async def runner() -> None:
            try:
                job.result = await loop.run_in_executor(
                    None, lambda: run_backtest(**kwargs, on_progress=on_progress)
                )
            except Exception as exc:
                job.error = str(exc)
            finally:
                job.ended_at_ns = time.time_ns()
                job.progress = 1.0

        job.task = asyncio.create_task(runner(), name=f"backtest-job-{job_id}")
        self.jobs[job_id] = job
        return job

    def status(self, job_id: str) -> dict[str, object] | None:
        job = self.jobs.get(job_id)
        if job is None:
            return None
        end_ns = job.ended_at_ns if job.ended_at_ns is not None else time.time_ns()
        return {
            "job_id": job.job_id,
            "done": job.ended_at_ns is not None,
            "elapsed_seconds": (end_ns - job.started_at_ns) / 1_000_000_000,
            "progress": job.progress,
            "error": job.error,
            "result": job.result,
        }


__all__ = ["BacktestJob", "BacktestJobManager"]

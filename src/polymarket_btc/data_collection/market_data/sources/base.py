"""Shared source utilities."""

from __future__ import annotations

from dataclasses import dataclass
import random


@dataclass(slots=True)
class ExponentialBackoff:
    initial: float = 0.5
    maximum: float = 10.0
    multiplier: float = 2.0
    jitter_fraction: float = 0.2
    _current: float | None = None

    def reset(self) -> None:
        self._current = None

    def next_delay(self) -> float:
        base = self.initial if self._current is None else min(
            self.maximum,
            self._current * self.multiplier,
        )
        self._current = base
        jitter = base * self.jitter_fraction
        return max(0.0, base + random.uniform(-jitter, jitter))

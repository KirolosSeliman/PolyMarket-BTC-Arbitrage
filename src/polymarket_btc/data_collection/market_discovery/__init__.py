"""Public API for deterministic BTC market discovery."""

from .client import GammaClient
from .models import (
    DiscoveryState,
    MarketWindow,
    ResolveOutcome,
    ResolveResult,
    Timeframe,
    TimeframeSnapshot,
    TransitionResult,
)
from .resolver import MarketResolver
from .transition import MarketDiscoveryRunner, TransitionController
from .transition_log import TransitionLogger

__all__ = [
    "Timeframe",
    "DiscoveryState",
    "MarketWindow",
    "ResolveOutcome",
    "ResolveResult",
    "TimeframeSnapshot",
    "TransitionResult",
    "GammaClient",
    "MarketResolver",
    "TransitionController",
    "TransitionLogger",
    "MarketDiscoveryRunner",
]

"""Vue terminal live pour le market data gateway (aucun navigateur requis).

Ce script lance sa PROPRE instance du gateway (comme `cli.py run`), donc
utilise-le à la place de `cli.py run`, pas en parallèle -- sinon tu ouvres
deux fois les connexions WebSocket et deux fois le dossier data/.

Installation:
    python -m pip install rich

Usage:
    python scripts/live_view.py --config config/market_data.toml
    python scripts/live_view.py --config config/market_data.toml --refresh-seconds 0.5
"""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from polymarket_btc.data_collection.market_data.config import load_config
from polymarket_btc.data_collection.market_data.gateway import MarketDataGateway
from polymarket_btc.data_collection.market_data.models import EventSource


def _fmt(value: object) -> str:
    return "—" if value is None else str(value)


def _status_label(health) -> str:
    if not health.connected:
        return "[red]DOWN[/]"
    if health.stale:
        return "[yellow]STALE[/]"
    return "[green]UP[/]"


def build_health_table(snapshot) -> Table:
    table = Table(title="Sources", expand=True)
    table.add_column("Source")
    table.add_column("Statut")
    table.add_column("Age (ms)")
    table.add_column("Reconnects")
    table.add_column("Invalides")

    health_by_source = dict(snapshot.health)
    for source, label in (
        (EventSource.CHAINLINK_RTDS, "Chainlink"),
        (EventSource.BINANCE_SPOT, "Binance"),
        (EventSource.POLYMARKET_CLOB, "CLOB"),
    ):
        health = health_by_source.get(source)
        if health is None:
            table.add_row(label, "[dim]n/a[/]", "—", "—", "—")
            continue
        table.add_row(
            label,
            _status_label(health),
            _fmt(health.age_ms),
            str(health.reconnect_count),
            str(health.invalid_count),
        )
    return table


def build_price_table(snapshot) -> Table:
    table = Table(title="Prix", expand=True)
    table.add_column("Métrique")
    table.add_column("Valeur")

    chainlink = snapshot.chainlink
    binance = snapshot.binance
    table.add_row("Chainlink BTC/USD", _fmt(chainlink.price))
    table.add_row("Binance last", _fmt(binance.last_price))
    table.add_row(
        "Binance bid / ask",
        f"{_fmt(binance.best_bid)} / {_fmt(binance.best_ask)}",
    )
    table.add_row("Binance mid", _fmt(binance.mid_price))
    table.add_row("Binance spread (bps)", _fmt(binance.spread_bps))

    for label, timeframe in (("5 min", snapshot.market_5m), ("15 min", snapshot.market_15m)):
        if timeframe is None:
            table.add_row(f"Marché {label}", "[dim]inactif[/]")
            continue
        up = timeframe.up
        down = timeframe.down
        up_str = f"{_fmt(up.best_bid if up else None)} / {_fmt(up.best_ask if up else None)}"
        down_str = f"{_fmt(down.best_bid if down else None)} / {_fmt(down.best_ask if down else None)}"
        table.add_row(f"{label} UP bid/ask", up_str)
        table.add_row(f"{label} DOWN bid/ask", down_str)
    return table


async def run(config_path: Path, refresh_seconds: float) -> None:
    config = load_config(config_path)
    gateway = MarketDataGateway(config)
    latest = None

    async def consume() -> None:
        nonlocal latest
        async for snapshot in gateway.snapshots():
            latest = snapshot

    async with gateway:
        consumer = asyncio.create_task(consume())
        try:
            with Live(refresh_per_second=4, screen=False) as live:
                while True:
                    if latest is not None:
                        status = (
                            "[bold green]READY[/]"
                            if latest.ready_for_strategy
                            else "[bold yellow]NOT READY[/]"
                        )
                        reasons = ", ".join(latest.not_ready_reasons) or "—"
                        header = Panel(
                            f"{status}   seq={latest.snapshot_sequence}   raisons: {reasons}",
                            title="Market Data Gateway",
                        )
                        live.update(
                            Group(header, build_health_table(latest), build_price_table(latest))
                        )
                    await asyncio.sleep(refresh_seconds)
        finally:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Vue terminal live du market data gateway")
    parser.add_argument("--config", type=Path, default=Path("config/market_data.toml"))
    parser.add_argument("--refresh-seconds", type=float, default=1.0)
    args = parser.parse_args()
    try:
        asyncio.run(run(args.config, args.refresh_seconds))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
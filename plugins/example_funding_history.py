"""Example data-collection plugin: Binance Futures funding rate history.

Not covered by the built-in feeds -- mark price/funding only exposes the
*current* rate, not the settled 8-hour history. This polls the public REST
endpoint every 5 minutes and appends one JSONL line per new entry to
`<run_dir>/funding_history.jsonl`.

Use this file as the template for your own plugin: copy it, rename it,
change PLUGIN_INFO and the body of run(). Anything placed in this
`plugins/` directory as a top-level .py file (not starting with `_`) that
defines PLUGIN_INFO and an async run(context) will show up as a checkbox
on the Data Collection page automatically -- no other wiring needed.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.request

from polymarket_btc.data_collection.control.plugins import PluginContext

PLUGIN_INFO = {
    "label": "Historique funding (exemple)",
    "category": "Crypto",
    "description": (
        "Interroge /fapi/v1/fundingRate (Binance Futures BTCUSDT) toutes les "
        "5 minutes et ecrit chaque nouveau reglement de funding en JSONL dans "
        "funding_history.jsonl."
    ),
    "detail": (
        "Donnee absente des flux integres, qui n'exposent que le taux de "
        "funding courant, pas l'historique des reglements. Sert aussi de "
        "modele pour illustrer le champ optionnel PLUGIN_INFO['detail'] : "
        "ce texte s'affiche dans la bulle qui apparait au clic sur le petit "
        "'i' a cote du plugin dans l'interface."
    ),
}

_URL = "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=10"
_POLL_SECONDS = 300.0


def _fetch() -> list[dict]:
    request = urllib.request.Request(_URL, headers={"User-Agent": "polymarket-btc-plugin"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


async def run(context: PluginContext) -> None:
    output_path = context.data_dir / "funding_history.jsonl"
    seen_times: set[int] = set()
    while not context.stop_event.is_set():
        try:
            rows = await asyncio.to_thread(_fetch)
            new_rows = [row for row in rows if int(row.get("fundingTime", -1)) not in seen_times]
            if new_rows:
                with output_path.open("a", encoding="utf-8") as handle:
                    for row in new_rows:
                        handle.write(json.dumps({
                            "collected_at_ns": time.time_ns(),
                            "symbol": row.get("symbol"),
                            "funding_time_ms": row.get("fundingTime"),
                            "funding_rate": row.get("fundingRate"),
                            "mark_price": row.get("markPrice"),
                        }) + "\n")
                        seen_times.add(int(row["fundingTime"]))
                context.log(f"funding_history: {len(new_rows)} nouvelle(s) ligne(s)")
        except Exception as exc:
            context.log(f"funding_history: erreur de poll: {exc!r}")
        try:
            await asyncio.wait_for(context.stop_event.wait(), timeout=_POLL_SECONDS)
        except TimeoutError:
            pass

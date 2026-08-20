"""Microsystème ICT : retournement FVG -> liquidity sweep -> FVG de retournement.

Combine les concepts `fvg` et `liquidity_sweep` pour reconnaître la séquence
en 3 temps d'un retournement engineered : un FVG initial (le move agressif
qui pousse le prix vers une zone de liquidité), un sweep confirmé de cette
liquidité (dont la direction du concept encode déjà le signal -- une BSL
balayée est "bearish", une SSL balayée est "bullish"), puis un nouveau FVG
dans le sens du sweep dont la première des 3 bougies est exactement la
bougie qui suit celle qui a réalisé le sweep.

Ni `fvg` ni `liquidity_sweep` ne renvoient les bougies elles-mêmes, seulement
des horodatages : `formed_at` d'un FVG est l'horodatage de sa 3e bougie,
`swept_at` d'un sweep est l'horodatage de la bougie qui a percé le niveau.
Avec `candle_seconds` (la granularité commune aux deux concepts), la bougie 1
du FVG de retournement doit tomber à `swept_at + candle_seconds`, donc son
`formed_at` (sa 3e bougie) doit tomber à `swept_at + 3 * candle_seconds`.
"""

from __future__ import annotations

MICROSYSTEM_INFO = {
    "label": "Retournement FVG + Liquidity Sweep (ICT)",
    "category": "ICT",
    "description": (
        "Détecte un retournement ICT complet : un FVG qui amorce un move "
        "agressif vers une zone de liquidité, un sweep de cette liquidité, "
        "puis un nouveau FVG en sens inverse formé juste après le sweep."
    ),
    "detail": (
        "Cherche, pour chaque sweep de liquidité confirmé, un FVG initial de "
        "direction opposée formé avant lui (le move agressif qui a mené vers "
        "la liquidité balayée) et un FVG de retournement dans le sens du "
        "sweep dont la première des 3 bougies est exactement la bougie "
        "suivant celle qui a réalisé le sweep. Exemple : FVG bearish (move "
        "vers le bas) -- sweep d'une sellside liquidity (signal bullish) -- "
        "FVG bullish formé juste après le sweep. Renvoie tous les setups "
        "complets détectés, triés du plus récent au plus ancien, avec un "
        "raccourci sur le signal le plus récent."
    ),
    "concept_inputs": ["fvg", "liquidity_sweep"],
    "config_schema": [
        {
            "name": "candle_seconds",
            "type": "number",
            "label": "Granularité des bougies (secondes)",
            "default": 5,
            "description": (
                "Doit correspondre exactement à la granularité de reconstruction "
                "des bougies utilisée pour configurer les concepts fvg et "
                "liquidity_sweep -- sert à calculer l'horodatage attendu de la "
                "bougie 1 du FVG de retournement."
            ),
        },
        {
            "name": "max_candles_before_sweep",
            "type": "number",
            "label": "Fenêtre de recherche du FVG initial (nb de bougies)",
            "default": 50,
            "description": (
                "Ne pas remonter plus loin que ce nombre de bougies avant le sweep "
                "pour lui associer un FVG initial de direction opposée."
            ),
        },
        {
            "name": "alignment_tolerance_candles",
            "type": "number",
            "label": "Tolérance d'alignement (nb de bougies)",
            "default": 0,
            "description": (
                "Tolérance, en nombre de bougies, autour de l'horodatage exact "
                "attendu pour la bougie 1 du FVG de retournement. 0 = alignement "
                "exact requis."
            ),
        },
    ],
}


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute(context) -> dict:
    fvg_result = context.concepts.get("fvg")
    if not isinstance(fvg_result, dict):
        fvg_result = {}
    sweep_result = context.concepts.get("liquidity_sweep")
    if not isinstance(sweep_result, dict):
        sweep_result = {}

    fvgs = [f for f in (fvg_result.get("fvgs") or []) if isinstance(f, dict)]
    sweeps = [s for s in (sweep_result.get("sweeps") or []) if isinstance(s, dict)]

    candle_seconds = _to_float(context.config.get("candle_seconds", 5)) or 5.0
    max_candles_before_sweep = max(
        int(_to_float(context.config.get("max_candles_before_sweep", 50)) or 50), 0
    )
    alignment_tolerance_candles = max(
        int(_to_float(context.config.get("alignment_tolerance_candles", 0)) or 0), 0
    )

    if not fvgs or not sweeps:
        context.log(
            f"{len(fvgs)} FVG actifs, {len(sweeps)} sweeps confirmés reçus -- "
            f"pas assez pour chercher un setup"
        )
        return {"setups": [], "dernier_signal": "neutre"}

    max_lookback_seconds = max_candles_before_sweep * candle_seconds
    tolerance_seconds = alignment_tolerance_candles * candle_seconds

    setups = []
    for sweep in sweeps:
        sweep_direction = sweep.get("direction")
        swept_at = _to_float(sweep.get("swept_at"))
        if sweep_direction not in ("bullish", "bearish") or swept_at is None:
            continue
        initial_direction = "bearish" if sweep_direction == "bullish" else "bullish"

        initial_candidates = [
            f for f in fvgs
            if f.get("direction") == initial_direction
            and _to_float(f.get("formed_at")) is not None
            and _to_float(f["formed_at"]) < swept_at
            and (swept_at - _to_float(f["formed_at"])) <= max_lookback_seconds
        ]
        if not initial_candidates:
            continue
        initial_fvg = max(initial_candidates, key=lambda f: _to_float(f["formed_at"]))

        expected_formed_at = swept_at + 3 * candle_seconds
        reversal_candidates = [
            f for f in fvgs
            if f.get("direction") == sweep_direction
            and _to_float(f.get("formed_at")) is not None
            and abs(_to_float(f["formed_at"]) - expected_formed_at) <= tolerance_seconds + 1e-9
        ]
        if not reversal_candidates:
            continue
        reversal_fvg = min(
            reversal_candidates,
            key=lambda f: abs(_to_float(f["formed_at"]) - expected_formed_at),
        )

        setups.append({
            "signal": "haussier" if sweep_direction == "bullish" else "baissier",
            "initial_fvg": initial_fvg,
            "sweep": sweep,
            "reversal_fvg": reversal_fvg,
            "detected_at": _to_float(reversal_fvg["formed_at"]),
        })

    setups.sort(key=lambda s: s["detected_at"], reverse=True)
    dernier_signal = setups[0]["signal"] if setups else "neutre"

    context.log(
        f"{len(fvgs)} FVG actifs, {len(sweeps)} sweeps confirmés -- "
        f"{len(setups)} setups détectés, dernier signal: {dernier_signal}"
    )

    return {"setups": setups, "dernier_signal": dernier_signal}

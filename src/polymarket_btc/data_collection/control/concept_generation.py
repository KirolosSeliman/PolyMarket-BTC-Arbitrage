"""Concept auto-generation via Claude Code (pilot): replaces the manual
"copy the prompt into an external AI, paste the script back" round-trip for
NEW concept creation with a background job that shells out to the `claude`
CLI in headless/print mode (`claude -p`), authenticated via the user's own
Claude Premium/Pro/Max subscription (CLAUDE_CODE_OAUTH_TOKEN, inherited from
this process's own environment -- nothing bundled or hardcoded here), and
feeds the result through the exact same import_concept_file validation a
manual upload already goes through.

This is NOT the same thing concept_refinement.py's own docstring documents
reverting (a direct Anthropic API call requiring a separate *paid* API
key) -- that concern still applies and is still avoided here. This uses
the user's own already-paid-for subscription via Claude Code's own CLI,
confirmed live by the user (a subscription-authenticated `claude -p` call
showed up as normal subscription usage in their account, not separate API
billing) before any of this was built. Started as a pilot scoped to
concept *creation* only -- `generate_concept_via_claude_code` is generic
(subprocess call + FILENAME:/code-block parsing, nothing concept-specific
about its behavior despite the name/module), so `concept_refinement.py`,
`microsystem_refinement.py`, and `strategy_filter_refinement.py` now reuse
it too, for their own auto-perfectionnement (see `auto_suffixed_filename`
below, and each manager's own `start_auto_refine_job`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import subprocess

from . import refinement
from .runs import CollectionRunManager

# The shared prompt doc (docs/nouveau_concept_prompt.md) is written for a
# human to read an AI's free-form reply and manually save it -- it has no
# machine-parseable output contract. This is the app's own addition, only
# ever appended to what's sent to Claude Code, never written back into that
# shared doc (which every other create/refine flow -- still copy-paste --
# also renders and must stay human-oriented).
_OUTPUT_CONTRACT = (
    "\n\n---\n\nRéponds uniquement avec :\n"
    "1. Une seule ligne exactement sous la forme : FILENAME: nom_du_fichier.py\n"
    "2. Un unique bloc de code ```python contenant le script complet et final.\n"
    "N'ajoute aucun autre texte, aucune explication, avant ou après."
)

_FILENAME_LINE_RE = re.compile(r"FILENAME:\s*(\S+\.py)", re.IGNORECASE)
_CODE_BLOCK_RE = re.compile(r"```(?:python)?\s*\n(.*?)```", re.DOTALL)


def _parse_claude_code_response(stdout: str) -> tuple[str, str]:
    filename_match = _FILENAME_LINE_RE.search(stdout)
    code_match = _CODE_BLOCK_RE.search(stdout)
    if not filename_match or not code_match:
        raise ValueError(
            "réponse de Claude Code illisible -- aucun nom de fichier ou bloc de code trouvé "
            "(essaie le prompt manuel ci-dessus à la place)"
        )
    return filename_match.group(1).strip(), code_match.group(1).strip() + "\n"


def auto_suffixed_filename(filename: str) -> str:
    """Used by every auto-perfectionnement flow (concept/microsystem/
    filter): never trust the model to avoid colliding with the original
    file it was asked to improve -- always rewrite the returned filename
    with an `_auto` suffix before import, regardless of what Claude Code
    itself returned. Strips any directory component; idempotent if the
    model already picked an `_auto`-suffixed name on its own."""
    stem = Path(filename).name.removesuffix(".py")
    if not stem.endswith("_auto"):
        stem += "_auto"
    return f"{stem}.py"


def generate_concept_via_claude_code(
    prompt: str, *, command: list[str], timeout_seconds: float,
) -> dict[str, str]:
    """Runs `*command -p "<prompt + output contract>" --disallowedTools "*"`
    -- the disallowedTools wildcard makes this pure text-in/text-out, zero
    filesystem/bash side effects from the subprocess itself, regardless of
    what the prompt asks for -- and parses stdout for a FILENAME: line plus
    one fenced python code block.

    stdin=DEVNULL: without it, the exact same command that completes in
    seconds when a user runs it by hand can hang until the timeout when
    launched from here -- the control server's own process doesn't give
    the child a real interactive stdin, and whatever Claude Code tries to
    read from it otherwise never gets EOF.

    Raises ValueError on a missing command, a timeout, a non-zero exit, or
    an unparseable response -- the same exception type _import_generic
    already maps to 400, kept consistent with every other bad-import error
    this app surfaces."""
    full_prompt = prompt + _OUTPUT_CONTRACT
    try:
        result = subprocess.run(
            [*command, "-p", full_prompt, "--disallowedTools", "*"],
            capture_output=True, text=True, timeout=timeout_seconds, stdin=subprocess.DEVNULL,
        )
    except FileNotFoundError as exc:
        raise ValueError(f"commande Claude Code introuvable ({command[0]!r}) : {exc}") from None
    except subprocess.TimeoutExpired:
        raise ValueError(f"Claude Code n'a pas répondu en {timeout_seconds:.0f}s -- réessaie") from None
    if result.returncode != 0:
        raise ValueError(f"Claude Code a échoué (code {result.returncode}) : {result.stderr.strip()[:500]}")
    filename, content = _parse_claude_code_response(result.stdout)
    return {"filename": filename, "content": content}


def expand_concept_description_via_claude_code(
    short_text: str, *, command: list[str], timeout_seconds: float,
) -> str:
    """A second, distinct Claude Code call from generate_concept_via_claude_
    code above -- that one is pure text-in/text-out with zero tool access
    (--disallowedTools "*"), deliberately, since it writes a script that
    gets imported and trusted. This one exists for the opposite case: the
    user typed a short/ambiguous term (e.g. "range breakout") instead of
    writing a full description themselves, and wants it researched and
    expanded. So it's deliberately given web search -- but nothing else:
    --permission-mode dontAsk denies anything not explicitly allowed, then
    --allowedTools "WebSearch" adds only that one tool back. No Bash, no
    file read/write/edit, ever. Returns plain descriptive text, never
    touches the filesystem, never feeds import_concept_file."""
    prompt = (
        "L'utilisateur envisage de créer un concept de trading algorithmique "
        "à partir de cette idée, éventuellement courte ou informelle :\n\n"
        f"{short_text}\n\n"
        "Si c'est un terme ou motif de trading connu (ex: range breakout, "
        "order block, fair value gap...), recherche sur internet ce qu'il "
        "signifie précisément pour t'assurer d'une définition correcte et à "
        "jour. Rédige ensuite une description complète (4 à 8 phrases) de ce "
        "que ce concept doit calculer ou détecter à partir de données de "
        "marché, avec des paramètres ajustables pertinents s'il y en a. "
        "Réponds uniquement avec cette description, sans préambule, sans "
        "conclusion, sans guillemets et sans mentionner la recherche."
    )
    try:
        result = subprocess.run(
            [*command, "-p", prompt, "--permission-mode", "dontAsk", "--allowedTools", "WebSearch"],
            capture_output=True, text=True, timeout=timeout_seconds, stdin=subprocess.DEVNULL,
        )
    except FileNotFoundError as exc:
        raise ValueError(f"commande Claude Code introuvable ({command[0]!r}) : {exc}") from None
    except subprocess.TimeoutExpired:
        raise ValueError(f"Claude Code n'a pas répondu en {timeout_seconds:.0f}s -- réessaie") from None
    if result.returncode != 0:
        raise ValueError(f"Claude Code a échoué (code {result.returncode}) : {result.stderr.strip()[:500]}")
    description = result.stdout.strip()
    if not description:
        raise ValueError("réponse de Claude Code vide -- réessaie ou écris la description toi-même")
    return description


# Only the sources backtest_data.py's own _ACCESS_EXTRACTORS/_COLLECT_
# EXTRACTORS actually know how to turn into records -- documented here
# (not derivable from CollectionRunManager._data_context_blocks, which
# only has label/description prose, no field-level shape) so the AI knows
# exactly what a single synthetic record must look like per source, the
# same way docs/nouveau_concept_prompt.md documents the CONCEPT_INFO
# contract for a concept's own *code*.
_SYNTHETIC_SHAPE_DOCS = {
    "binance_futures_kline": "champs : open, high, low, close, volume (float), "
        "timestamp, open_time, close_time (float, secondes unix), is_closed (bool)",
    "binance_futures_trade": "champs : price, quantity (float), timestamp (float, secondes unix), "
        "taker_side (\"buy\" ou \"sell\")",
    "binance_futures_mark_price": "champs : mark_price, index_price, funding_rate (float), "
        "timestamp (float, secondes unix)",
    "chainlink": "champs : price (float), timestamp (float, secondes unix)",
    "binance_spot": "champs : price (float), timestamp (float, secondes unix)",
}


def generate_synthetic_example_via_claude_code(
    concept_source: str, data_sources: list[str], *, command: list[str], timeout_seconds: float,
) -> dict[str, list[dict]]:
    """A third, distinct Claude Code call -- for when there's too little
    (or no) real collected data to find a real instance to review on
    Perfectionner. Reads the concept's own source and invents a plausible,
    textbook-quality synthetic scenario of what it's meant to detect, as
    raw records shaped like this app's own real data (see
    _SYNTHETIC_SHAPE_DOCS) -- the concept's real compute() then decides
    what's detected, not the AI's own claim. Pure text-in/JSON-out,
    --disallowedTools "*" like generate_concept_via_claude_code -- this
    generates fake DATA, not a script, so no filesystem/tool access is
    needed here either."""
    shape_blocks = []
    for key in data_sources:
        doc = _SYNTHETIC_SHAPE_DOCS.get(key)
        shape_blocks.append(f"- `{key}` : {doc}" if doc else f"- `{key}` : type de donnée non supporté, ignore-le")
    prompt = (
        "Voici le code source d'un concept de trading algorithmique :\n\n"
        f"```python\n{concept_source}\n```\n\n"
        "Invente un exemple de données brutes SYNTHÉTIQUES (fictives mais réalistes) "
        "représentant un cas d'école clair et net de ce que ce concept est censé "
        "détecter -- assez de records, sur une plage de temps assez longue, pour que "
        "compute() puisse effectivement le détecter avec les paramètres par défaut du "
        "concept. Types de données à générer (une liste de records par type) :\n"
        + "\n".join(shape_blocks)
        + "\n\nRéponds uniquement avec un objet JSON de la forme "
        '{"<clé de la source>": [ {...}, {...}, ... ], ...} -- records triés par '
        "timestamp croissant. Pas de texte avant ou après, pas de bloc de code, "
        "juste le JSON brut."
    )
    try:
        result = subprocess.run(
            [*command, "-p", prompt, "--disallowedTools", "*"],
            capture_output=True, text=True, timeout=timeout_seconds, stdin=subprocess.DEVNULL,
        )
    except FileNotFoundError as exc:
        raise ValueError(f"commande Claude Code introuvable ({command[0]!r}) : {exc}") from None
    except subprocess.TimeoutExpired:
        raise ValueError(f"Claude Code n'a pas répondu en {timeout_seconds:.0f}s -- réessaie") from None
    if result.returncode != 0:
        raise ValueError(f"Claude Code a échoué (code {result.returncode}) : {result.stderr.strip()[:500]}")
    stdout = result.stdout.strip()
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError:
        raise ValueError(
            "réponse de Claude Code illisible -- pas du JSON valide (réessaie, ou collecte de vraies données)"
        ) from None
    if not isinstance(parsed, dict) or not parsed:
        raise ValueError("réponse de Claude Code invalide -- attendu un objet JSON non vide de listes de records")
    return parsed


@dataclass(slots=True)
class ConceptGenerationManager:
    runs: CollectionRunManager
    command: list[str]
    timeout_seconds: float
    jobs: dict[str, refinement.ScanJob] = field(default_factory=dict)

    def _generate(self, *, prompt: str, overwrite: bool) -> dict[str, object]:
        generated = generate_concept_via_claude_code(
            prompt, command=self.command, timeout_seconds=self.timeout_seconds,
        )
        # A name collision surfaces as a plain FileExistsError message
        # through the job's own `error` field -- no special 409 path for
        # the background-job route (unlike the direct manual-import
        # route). The frontend parses that message the same way
        # createImportPicker's own confirm()-to-overwrite flow already
        # does, then resubmits with overwrite=True.
        return self.runs.import_concept_file(generated["filename"], generated["content"], overwrite=overwrite)

    def start_generate_job(self, *, prompt: str, overwrite: bool = False) -> refinement.ScanJob:
        return refinement.run_scan_job(
            self.jobs, lambda _on_progress: self._generate(prompt=prompt, overwrite=overwrite),
            name_prefix="concept-generate-job",
        )

    def generate_job_status(self, job_id: str) -> dict[str, object] | None:
        return refinement.scan_job_status(self.jobs, job_id)

    def _expand_description(self, *, short_text: str) -> dict[str, object]:
        description = expand_concept_description_via_claude_code(
            short_text, command=self.command, timeout_seconds=self.timeout_seconds,
        )
        return {"description": description}

    def start_expand_description_job(self, *, short_text: str) -> refinement.ScanJob:
        return refinement.run_scan_job(
            self.jobs, lambda _on_progress: self._expand_description(short_text=short_text),
            name_prefix="concept-expand-description-job",
        )

    def expand_description_job_status(self, job_id: str) -> dict[str, object] | None:
        return refinement.scan_job_status(self.jobs, job_id)


__all__ = [
    "ConceptGenerationManager", "auto_suffixed_filename", "expand_concept_description_via_claude_code",
    "generate_concept_via_claude_code", "generate_synthetic_example_via_claude_code",
]

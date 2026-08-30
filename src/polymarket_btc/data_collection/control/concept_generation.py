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
billing) before any of this was built. Scoped to concept *creation* only
(a pilot) -- concept_refinement.py's own "perfectionner" flow, and every
other create/refine prompt in this app, is untouched; if this pilot works
out, the same mechanism can extend to those later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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


def generate_concept_via_claude_code(
    prompt: str, *, command: list[str], timeout_seconds: float,
) -> dict[str, str]:
    """Runs `*command -p "<prompt + output contract>" --disallowedTools "*"`
    -- the disallowedTools wildcard makes this pure text-in/text-out, zero
    filesystem/bash side effects from the subprocess itself, regardless of
    what the prompt asks for -- and parses stdout for a FILENAME: line plus
    one fenced python code block.

    Raises ValueError on a missing command, a timeout, a non-zero exit, or
    an unparseable response -- the same exception type _import_generic
    already maps to 400, kept consistent with every other bad-import error
    this app surfaces."""
    full_prompt = prompt + _OUTPUT_CONTRACT
    try:
        result = subprocess.run(
            [*command, "-p", full_prompt, "--disallowedTools", "*"],
            capture_output=True, text=True, timeout=timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise ValueError(f"commande Claude Code introuvable ({command[0]!r}) : {exc}") from None
    except subprocess.TimeoutExpired:
        raise ValueError(f"Claude Code n'a pas répondu en {timeout_seconds:.0f}s -- réessaie") from None
    if result.returncode != 0:
        raise ValueError(f"Claude Code a échoué (code {result.returncode}) : {result.stderr.strip()[:500]}")
    filename, content = _parse_claude_code_response(result.stdout)
    return {"filename": filename, "content": content}


@dataclass(slots=True)
class ConceptGenerationManager:
    runs: CollectionRunManager
    command: list[str]
    timeout_seconds: float
    jobs: dict[str, refinement.ScanJob] = field(default_factory=dict)

    def _generate(self, *, sources: list[str], plugins: list[str], template: str) -> dict[str, object]:
        prompt = self.runs.build_concept_prompt(sources=sources, plugins=plugins, template=template)
        generated = generate_concept_via_claude_code(
            prompt, command=self.command, timeout_seconds=self.timeout_seconds,
        )
        # overwrite=False: a name collision surfaces as a plain FileExistsError
        # message through the job's own `error` field -- no special 409 path
        # for the background-job route (unlike the direct manual-import
        # route), an accepted simplification for this pilot.
        return self.runs.import_concept_file(generated["filename"], generated["content"], overwrite=False)

    def start_generate_job(self, *, sources: list[str], plugins: list[str], template: str) -> refinement.ScanJob:
        return refinement.run_scan_job(
            self.jobs,
            lambda _on_progress: self._generate(sources=sources, plugins=plugins, template=template),
            name_prefix="concept-generate-job",
        )

    def generate_job_status(self, job_id: str) -> dict[str, object] | None:
        return refinement.scan_job_status(self.jobs, job_id)


__all__ = ["ConceptGenerationManager", "generate_concept_via_claude_code"]

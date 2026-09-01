import asyncio
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from polymarket_btc.data_collection.control.concept_generation import (
    ConceptGenerationManager,
    _parse_claude_code_response,
    expand_concept_description_via_claude_code,
    generate_concept_via_claude_code,
)
from polymarket_btc.data_collection.control.runs import CollectionRunManager

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

_WELL_FORMED_RESPONSE = '''FILENAME: my_new_concept.py

```python
CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}

def compute(context):
    return {"last_close": None}
```
'''


class ParseClaudeCodeResponseTests(unittest.TestCase):
    def test_well_formed_response_extracts_filename_and_content(self) -> None:
        filename, content = _parse_claude_code_response(_WELL_FORMED_RESPONSE)
        self.assertEqual(filename, "my_new_concept.py")
        self.assertIn("CONCEPT_INFO", content)
        self.assertIn("def compute(context):", content)
        self.assertTrue(content.endswith("\n"))

    def test_missing_filename_line_raises(self) -> None:
        response = "```python\nCONCEPT_INFO = {}\n```"
        with self.assertRaises(ValueError):
            _parse_claude_code_response(response)

    def test_missing_code_block_raises(self) -> None:
        response = "FILENAME: foo.py\n\nJust some prose, no code block."
        with self.assertRaises(ValueError):
            _parse_claude_code_response(response)

    def test_filename_without_py_extension_is_rejected(self) -> None:
        response = "FILENAME: foo\n```python\nx = 1\n```"
        with self.assertRaises(ValueError):
            _parse_claude_code_response(response)


class GenerateConceptViaClaudeCodeTests(unittest.TestCase):
    def test_success_returns_filename_and_content(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=_WELL_FORMED_RESPONSE, stderr="",
            )
            result = generate_concept_via_claude_code("un prompt", command=["claude"], timeout_seconds=30)
        self.assertEqual(result["filename"], "my_new_concept.py")
        self.assertIn("CONCEPT_INFO", result["content"])

    def test_invokes_with_disallowed_tools_wildcard_and_print_flag(self) -> None:
        # Safety-critical: this call must be pure text-in/text-out, never
        # allowed to touch the filesystem or run commands on its own
        # initiative -- confirmed via the actual argv passed to subprocess.run.
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=_WELL_FORMED_RESPONSE, stderr="",
            )
            generate_concept_via_claude_code("un prompt", command=["claude"], timeout_seconds=30)
        called_args = mock_run.call_args[0][0]
        self.assertIn("-p", called_args)
        self.assertIn("--disallowedTools", called_args)
        self.assertIn("*", called_args)
        # shell=True must never be used -- the prompt is passed as a single
        # argv element, never interpolated into a shell string.
        self.assertNotIn("shell", mock_run.call_args[1])
        # stdin=DEVNULL: found live -- without this, the subprocess can hang
        # indefinitely (until the timeout) waiting on stdin when launched
        # from the control server's own process instead of an interactive
        # terminal, even though the exact same command completes in
        # seconds when run by hand.
        self.assertEqual(mock_run.call_args[1].get("stdin"), subprocess.DEVNULL)

    def test_multi_word_command_is_used_as_given(self) -> None:
        # e.g. ["npx", "@anthropic-ai/claude-code"] for an environment where
        # "claude" isn't directly on PATH (confirmed by the user's own setup).
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=_WELL_FORMED_RESPONSE, stderr="",
            )
            generate_concept_via_claude_code(
                "un prompt", command=["npx", "@anthropic-ai/claude-code"], timeout_seconds=30,
            )
        called_args = mock_run.call_args[0][0]
        self.assertEqual(called_args[:2], ["npx", "@anthropic-ai/claude-code"])

    def test_nonzero_exit_raises_with_stderr(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="authentication failed",
            )
            with self.assertRaises(ValueError) as ctx:
                generate_concept_via_claude_code("un prompt", command=["claude"], timeout_seconds=30)
            self.assertIn("authentication failed", str(ctx.exception))

    def test_timeout_raises_clearly(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="claude", timeout=30)
            with self.assertRaises(ValueError) as ctx:
                generate_concept_via_claude_code("un prompt", command=["claude"], timeout_seconds=30)
            self.assertIn("30", str(ctx.exception))

    def test_missing_command_raises_clearly(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError("no such file")
            with self.assertRaises(ValueError) as ctx:
                generate_concept_via_claude_code("un prompt", command=["claude"], timeout_seconds=30)
            self.assertIn("claude", str(ctx.exception))

    def test_unparseable_response_raises(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="no filename or code block here", stderr="",
            )
            with self.assertRaises(ValueError):
                generate_concept_via_claude_code("un prompt", command=["claude"], timeout_seconds=30)


class ExpandConceptDescriptionViaClaudeCodeTests(unittest.TestCase):
    def test_success_returns_stripped_description(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="  Une description complète.  \n", stderr="",
            )
            result = expand_concept_description_via_claude_code(
                "range breakout", command=["claude"], timeout_seconds=30,
            )
        self.assertEqual(result, "Une description complète.")

    def test_invokes_with_web_search_only_and_dont_ask_mode(self) -> None:
        # Safety-critical: unlike generate_concept_via_claude_code (zero
        # tool access), this call is deliberately given web search -- but
        # confirm it's scoped to exactly that, not a bare grant of every
        # tool. See expand_concept_description_via_claude_code's docstring.
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="une description", stderr="",
            )
            expand_concept_description_via_claude_code(
                "range breakout", command=["claude"], timeout_seconds=30,
            )
        called_args = mock_run.call_args[0][0]
        self.assertIn("-p", called_args)
        self.assertIn("--permission-mode", called_args)
        self.assertEqual(called_args[called_args.index("--permission-mode") + 1], "dontAsk")
        self.assertIn("--allowedTools", called_args)
        self.assertEqual(called_args[called_args.index("--allowedTools") + 1], "WebSearch")
        self.assertNotIn("--disallowedTools", called_args)
        self.assertNotIn("shell", mock_run.call_args[1])
        self.assertEqual(mock_run.call_args[1].get("stdin"), subprocess.DEVNULL)

    def test_short_text_reaches_the_prompt(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="une description", stderr="",
            )
            expand_concept_description_via_claude_code(
                "range breakout", command=["claude"], timeout_seconds=30,
            )
        called_args = mock_run.call_args[0][0]
        prompt = called_args[called_args.index("-p") + 1]
        self.assertIn("range breakout", prompt)

    def test_nonzero_exit_raises_with_stderr(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="authentication failed",
            )
            with self.assertRaises(ValueError) as ctx:
                expand_concept_description_via_claude_code(
                    "range breakout", command=["claude"], timeout_seconds=30,
                )
            self.assertIn("authentication failed", str(ctx.exception))

    def test_empty_response_raises(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="   \n", stderr="",
            )
            with self.assertRaises(ValueError):
                expand_concept_description_via_claude_code(
                    "range breakout", command=["claude"], timeout_seconds=30,
                )

    def test_timeout_raises_clearly(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="claude", timeout=30)
            with self.assertRaises(ValueError) as ctx:
                expand_concept_description_via_claude_code(
                    "range breakout", command=["claude"], timeout_seconds=30,
                )
            self.assertIn("30", str(ctx.exception))


class ConceptGenerationManagerTests(unittest.IsolatedAsyncioTestCase):
    # start_generate_job schedules its background work via asyncio.create_
    # task (see refinement.run_scan_job), which requires a running event
    # loop -- IsolatedAsyncioTestCase, same pattern test_server.py's own
    # job-creation tests already use, not plain TestCase.

    def _setup(self) -> tuple[ConceptGenerationManager, Path]:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        (root / "concepts").mkdir(parents=True)
        runs = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            plugins_dir=root / "plugins", concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems", execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles", filter_dir=root / "filter_profiles",
        )
        manager = ConceptGenerationManager(runs=runs, command=["claude"], timeout_seconds=30)
        return manager, root

    async def _wait_for_done(self, manager: ConceptGenerationManager, job_id: str) -> dict[str, object]:
        for _ in range(100):
            status = manager.generate_job_status(job_id)
            if status["done"]:
                return status
            await asyncio.sleep(0.02)
        self.fail("job never completed")

    async def test_full_job_round_trip_imports_the_generated_file(self) -> None:
        manager, root = self._setup()
        with patch(
            "polymarket_btc.data_collection.control.concept_generation.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = {
                "filename": "my_new_concept.py",
                "content": _WELL_FORMED_RESPONSE.split("```python\n")[1].split("```")[0],
            }
            job = manager.start_generate_job(prompt="un prompt déjà construit et confirmé par l'utilisateur")
            status = await self._wait_for_done(manager, job.job_id)
        self.assertIsNone(status["error"])
        self.assertEqual(status["result"]["filename"], "my_new_concept.py")
        self.assertTrue((root / "concepts" / "my_new_concept.py").is_file())

    async def test_job_error_surfaces_through_status_without_raising(self) -> None:
        manager, _root = self._setup()
        with patch(
            "polymarket_btc.data_collection.control.concept_generation.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.side_effect = ValueError("Claude Code a échoué")
            job = manager.start_generate_job(prompt="un prompt déjà construit et confirmé par l'utilisateur")
            status = await self._wait_for_done(manager, job.job_id)
        self.assertIn("échoué", status["error"])

    async def test_unknown_job_id_returns_none(self) -> None:
        manager, _root = self._setup()
        self.assertIsNone(manager.generate_job_status("no-such-job"))

    async def test_expand_description_job_round_trip(self) -> None:
        manager, _root = self._setup()
        with patch(
            "polymarket_btc.data_collection.control.concept_generation.expand_concept_description_via_claude_code",
        ) as mock_expand:
            mock_expand.return_value = "Une description complète du concept."
            job = manager.start_expand_description_job(short_text="range breakout")
            for _ in range(100):
                status = manager.expand_description_job_status(job.job_id)
                if status["done"]:
                    break
                await asyncio.sleep(0.02)
            else:
                self.fail("job never completed")
        self.assertIsNone(status["error"])
        self.assertEqual(status["result"]["description"], "Une description complète du concept.")

    async def test_expand_description_job_error_surfaces_through_status(self) -> None:
        manager, _root = self._setup()
        with patch(
            "polymarket_btc.data_collection.control.concept_generation.expand_concept_description_via_claude_code",
        ) as mock_expand:
            mock_expand.side_effect = ValueError("Claude Code a échoué")
            job = manager.start_expand_description_job(short_text="range breakout")
            for _ in range(100):
                status = manager.expand_description_job_status(job.job_id)
                if status["done"]:
                    break
                await asyncio.sleep(0.02)
            else:
                self.fail("job never completed")
        self.assertIn("échoué", status["error"])

    async def test_expand_description_unknown_job_id_returns_none(self) -> None:
        manager, _root = self._setup()
        self.assertIsNone(manager.expand_description_job_status("no-such-job"))


if __name__ == "__main__":
    unittest.main()

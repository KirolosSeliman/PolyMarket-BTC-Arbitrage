# Market Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a read-only, configurable Market Discovery module for Polymarket BTC Up/Down five-minute markets.

**Architecture:** Create a small Python package under `src/polymarket_btc` with a shared `data_collection/common` layer and an isolated `data_collection/market_discovery` module. Use Gamma public endpoints only, normalize external payloads into typed dataclasses, and fail closed on missing or ambiguous metadata.

**Tech Stack:** Python 3.13, standard library `unittest`, PyYAML 6.0.3, `urllib.request` for bounded public HTTP calls.

## Global Constraints

- No trading, wallet, private key, order placement, order sizing, probability model, backtest, or database.
- All market time logic uses timezone-aware UTC datetimes.
- Unit tests must not require network access.
- Outcomes and CLOB token IDs must be parsed and mapped by matching indexes only after collection lengths and values are validated.
- `active=true` is never sufficient for selecting a market.
- BTC 5m selection uses `eventStartTime <= now < endDate`; Gamma `startDate` is not the five-minute window start for this market family.
- Runtime network audit must be optional, read-only, timeout-bound, and require no API key.

---

### Task 1: Project Scaffolding And Config Validation

**Files:**
- Create: `pyproject.toml`
- Create: `src/polymarket_btc/__init__.py`
- Create: `src/polymarket_btc/data_collection/__init__.py`
- Create: `src/polymarket_btc/data_collection/common/__init__.py`
- Create: `src/polymarket_btc/data_collection/market_discovery/__init__.py`
- Create: `src/polymarket_btc/data_collection/market_discovery/config.py`
- Create: `config/data_collection/market_discovery.yaml`
- Create: `tests/data_collection/market_discovery/test_config.py`

**Interfaces:**
- Produces: `load_market_discovery_config(path: Path) -> MarketDiscoveryConfig`
- Produces: `MarketDiscoveryConfig`, `ProviderConfig`, `TargetConfig`, `PollingConfig`, `SelectionConfig`, `FailurePolicyConfig`

- [ ] Write config tests for valid YAML, missing required sections, invalid target duration, unknown top-level keys, invalid HTTPS base URL, and invalid polling values.
- [ ] Run `python -m unittest tests.data_collection.market_discovery.test_config` and verify expected import/feature failures.
- [ ] Implement frozen dataclass config models and strict validation.
- [ ] Add default config YAML with no dynamic market identifiers.
- [ ] Run the config tests and verify they pass.

### Task 2: Models, Fixtures, And Payload Normalization

**Files:**
- Create: `src/polymarket_btc/data_collection/common/time.py`
- Create: `src/polymarket_btc/data_collection/market_discovery/models.py`
- Create: `src/polymarket_btc/data_collection/market_discovery/normalizer.py`
- Create: `tests/data_collection/market_discovery/fixtures.py`
- Create: `tests/data_collection/market_discovery/test_normalizer.py`

**Interfaces:**
- Consumes: `MarketDiscoveryConfig`
- Produces: `parse_utc_datetime(value: object, field_name: str) -> datetime`
- Produces: `normalize_gamma_market(payload: Mapping[str, Any], config: MarketDiscoveryConfig, observed_at_utc: datetime) -> CandidateValidation`
- Produces: `OutcomeDescriptor`, `MarketDescriptor`, `RejectedCandidate`, `CandidateValidation`

- [ ] Write tests for JSON-string outcomes, array outcomes, mismatched outcome/token counts, duplicate outcomes, duplicate tokens, unknown outcomes, missing condition ID, missing token IDs, invalid dates, and accepted BTC 5m payloads.
- [ ] Run the normalizer tests and verify expected feature failures.
- [ ] Implement UTC parsing, JSON list parsing, outcome normalization, token mapping, and candidate rejection reasons.
- [ ] Run the normalizer tests and verify they pass.

### Task 3: Active And Next Market Selection

**Files:**
- Create: `src/polymarket_btc/data_collection/market_discovery/selector.py`
- Create: `tests/data_collection/market_discovery/test_selector.py`

**Interfaces:**
- Consumes: `CandidateValidation`, `MarketDescriptor`, `MarketDiscoveryConfig`
- Produces: `select_markets(candidates: Sequence[CandidateValidation], now_utc: datetime, config: MarketDiscoveryConfig) -> DiscoveryResult`
- Produces: `DiscoveryResult`, `DiscoveryStatus`

- [ ] Write tests for a single current market, no current market, multiple current markets, future active market, closed market, exactly at start, one millisecond before start, exactly at end, one millisecond before end, detected next market, and missing next market.
- [ ] Run selector tests and verify expected feature failures.
- [ ] Implement strict current and next selection with start-inclusive/end-exclusive boundaries.
- [ ] Run selector tests and verify they pass.

### Task 4: Gamma Client, Retry Policy, And Service

**Files:**
- Create: `src/polymarket_btc/data_collection/common/http.py`
- Create: `src/polymarket_btc/data_collection/market_discovery/gamma_client.py`
- Create: `src/polymarket_btc/data_collection/market_discovery/service.py`
- Create: `tests/data_collection/market_discovery/test_gamma_client.py`
- Create: `tests/data_collection/market_discovery/test_service.py`

**Interfaces:**
- Consumes: `MarketDiscoveryConfig`, `select_markets`, `normalize_gamma_market`
- Produces: `GammaClient.fetch_market_by_slug(slug: str) -> Mapping[str, Any] | None`
- Produces: `GammaClient.search_btc_five_minute_markets(limit: int) -> list[Mapping[str, Any]]`
- Produces: `MarketDiscoveryService.discover_once(now_utc: datetime | None = None) -> DiscoveryResult`
- Produces: `MarketDiscoveryService.poll(iterations: int | None = None) -> Iterator[DiscoveryResult]`

- [ ] Write fake-transport tests for successful slug fetch, 404 no match, retry on 500, retry on 429, no retry loop on 400, timeout conversion, slug generation around five-minute boundaries, transition detection, and no duplicate transition.
- [ ] Run client/service tests and verify expected feature failures.
- [ ] Implement bounded HTTP requests, retry/backoff policy, computed current/next slug candidates, optional public-search audit, service one-shot, and transition tracking.
- [ ] Run client/service tests and verify they pass.

### Task 5: CLI, Documentation, And README

**Files:**
- Create: `src/polymarket_btc/data_collection/market_discovery/cli.py`
- Create: `tests/data_collection/market_discovery/test_cli.py`
- Create: `docs/data_collection/market_discovery.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: `MarketDiscoveryService`, `load_market_discovery_config`
- Produces: `python -m polymarket_btc.data_collection.market_discovery.cli --validate-config`
- Produces: `python -m polymarket_btc.data_collection.market_discovery.cli --once --json`
- Produces: `python -m polymarket_btc.data_collection.market_discovery.cli --watch --iterations N --json`
- Produces: `python -m polymarket_btc.data_collection.market_discovery.cli --network-audit --json`

- [ ] Write CLI tests for config validation success, invalid config failure, fixture one-shot JSON output, and no-match exit behavior.
- [ ] Run CLI tests and verify expected feature failures.
- [ ] Implement CLI argument parsing, JSON serialization, exit codes, fixture input, one-shot, watch, and optional network audit mode.
- [ ] Document the architecture, endpoint choice, reliable fields, outcome/token mapping, time rules, errors, tests, and limits.
- [ ] Update README with the project scope and commands.
- [ ] Run CLI tests and verify they pass.

### Task 6: Full Verification And Red-Team Review

**Files:**
- No new files expected.

**Interfaces:**
- Consumes all previous tasks.
- Produces final audit report.

- [ ] Run `python -m unittest`.
- [ ] Run `python -m compileall src tests`.
- [ ] Run `python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --validate-config`.
- [ ] Run fixture-backed one-shot CLI validation.
- [ ] If network is allowed, run optional network audit with a short timeout.
- [ ] Run `git diff --check`.
- [ ] Review ambiguity, future-active candidates, malformed payloads, duplicate tokens, closed markets, date boundaries, and network failure paths.
- [ ] Record remaining limitations and exact command results.

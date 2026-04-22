# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

Context and conventions for this project. Read before generating code.

## Project purpose

Thin wrapper around the `microtrade` Python library. Runs as a Kubernetes
`CronJob`. Each run:

1. Generates microtrade spec YAMLs from workbook files (stage 1).
2. Ingests raw zipped data files into hive-partitioned parquet (stage 2).
3. Records state to local disk so subsequent runs skip already-done work.

All heavy lifting (parsing workbooks, reading raw files, writing parquet) is
done by `microtrade`. This project is planning, dispatch, and state tracking.

**Raw drop semantics (microtrade's model):** each raw file is a YTD snapshot
for its `(trade_type, year)`. A single `..._202406N.TXT.zip` file covers
Jan–Jun of 2024; microtrade partitions rows internally via each sheet's
`routing_column` (a row-level Date). One raw file therefore produces many
month partitions, and the latest snapshot per `(trade_type, year)` wins at
ingest time. The unit of reprocessing in this project is
`(trade_type, year)`, not a single month.

## Stack

- Python 3.12+ (microtrade requires `>=3.12`)
- `loguru` for logging
- `pydantic-settings` for project config (env + `.env`)
- `pydantic` v2 for validating `microtrade.yaml`
- `pyyaml` for YAML parsing
- `microtrade` (imported as a library, not shelled out)
- No orchestrator — k8s `CronJob` is the scheduler

No Airflow, Prefect, Dagster, Datadog, or any long-running server. No network
services. State lives on disk only.

## Code conventions

- Type hints on all public functions.
- Functions over classes unless state or a protocol is needed.
- Keep modules small and single-purpose.
- Fail fast: validate config at startup, raise on unexpected input.
- No silent excepts. Log and re-raise or log and continue with explicit intent.

## Logging

Use `loguru` with its defaults. No custom sinks, no structured JSON, no
aggregator integration.

```python
from loguru import logger
logger.info("message")
```

Add a rotating file sink only if the user asks for persistent logs.

## Config

Two separate YAMLs, two roles:

- **`config.yaml` + env vars** — *where*: paths, directories.
  Loaded via `pydantic-settings`. Changes per environment.
- **`microtrade.yaml`** — *how*: microtrade's own domain config (workbooks,
  sheets, filename patterns, column casts/parses/renames). Read directly by
  microtrade. This project *also* parses it with pydantic models, but only
  for planning (matching raw files to partitions).

Environment variable prefix: `MT_`. Example: `MT_RAW_DIR=/data/raw`.

### `config.yaml` fields

```
microtrade_yaml: path to microtrade.yaml
workbooks_dir: directory of .xls workbook files (stage 1 input)
raw_dir: directory of raw zipped data files (stage 2 input)
specs_dir: directory where microtrade writes generated spec YAMLs
processed_dir: directory for hive-partitioned parquet output
spec_manifests_dir: state dir for stage 1
raw_manifests_dir: state dir for stage 2 (one JSON per raw file)
upstream_raw_dir: remote source (provider drops here, periodically deletes)
raw_remote_dir: our permanent archive (mirror of upstream + version history)
```

## State tracking

All state lives on disk as JSON files (one per tracked item). No SQLite, no
JSONL append log, no database. Rationale: simplest possible model, easy to
inspect with `cat`/`jq`, no concurrency concerns for a single-pod cronjob.

Write atomically: write to `path.tmp`, then `os.replace(tmp, path)`.

### Two manifest directories

```
data/manifests/
  specs/        # one JSON per workbook file
  raw/          # one JSON per raw file
```

### Spec manifest (stage 1) fields

- `workbook_name`
- `workbook_hash` (content hash of the workbook file)
- `microtrade_hash` (content hash of `microtrade.yaml` at time of generation)
- `specs_written` (list of output spec file paths)
- `processed_at` (ISO-8601 UTC)

### Raw manifest (stage 2) fields

- `raw_name`
- `raw_hash` (content hash of the raw zip)
- `microtrade_hash` (content hash of `microtrade.yaml` at time of ingest)
- `trade_type`, `year`, `month`, `flag` (extracted from filename via
  `filename_pattern` — `month` is the snapshot month, not a partition key)
- `processed_at` (ISO-8601 UTC)

Rows written, per-partition paths, and per-row quality issues are owned by
microtrade's own manifest under `processed_dir/_manifests/<trade_type>/`.
This project does not duplicate them.

## Planning (dirty-check) logic

### Stage 1 — spec generation

A workbook is dirty if:
- no manifest exists, OR
- `workbook_hash` differs from current file hash, OR
- `microtrade_hash` differs from current `microtrade.yaml` hash.

### Stage 2 — year ingest

A raw file is dirty if:
- no manifest exists, OR
- `raw_hash` differs from current file hash, OR
- `microtrade_hash` differs from current `microtrade.yaml` hash.

If any raw file mapping to a given `(trade_type, year)` is dirty, the whole
year is dirty for that trade type. Grouping key: `(trade_type, year)`.
Rationale: microtrade reprocesses at year granularity (one YTD snapshot
drives Jan..snapshot-month); there is no "single month" unit to reprocess.

## Year reprocessing model

Output layout is hive-partitioned:
`<trade_type>/year=YYYY/month=MM/part-N.parquet`

When `(trade_type=T, year=Y)` is dirty:

1. Call `microtrade.pipeline.run(PipelineConfig(input_dir=raw_dir,
   output_dir=processed_dir, spec_dir=specs_dir, trade_types=(T,), year=Y,
   ytd=False))`.
2. Microtrade handles discovery, latest-snapshot-per-year selection,
   partition atomicity (`.tmp` + rename), and delete-before-rewrite
   internally. This project does not touch `processed_dir` directly.
3. On success, write raw manifests for every raw file that maps to
   `(T, Y)`. On failure (non-zero `failed_count` in `RunSummary`), skip
   manifest updates so the year replans next run.

Self-healing on partial failure: failed years have no manifest updates and
replan automatically.

## Matching raw files to partitions

`microtrade.yaml` declares workbooks, each with sheets, each with a
`filename_pattern` (regex with named groups `year`, `month`, `flag`) and an
`effective_from`/`effective_to` date window.

To match a raw file: iterate workbooks and sheets, test the filename regex,
extract `year`/`month`/`flag` from the match, check the date falls within the
workbook's effective window. First match wins. Date windows do not overlap
(guaranteed by the project spec), so first-match is deterministic.

A raw file with no matching sheet is logged as a warning and skipped.

## Microtrade adapter

Microtrade is called through a thin adapter class so the pipeline code
doesn't depend on microtrade's exact API shape. The adapter exposes two
methods:

```python
class MicrotradeAdapter:
    def import_spec(self, workbook: Path, microtrade_yaml: Path,
                    specs_out: Path) -> list[Path]: ...
    def ingest_year(self, trade_type: str, year: int, raw_dir: Path,
                    specs_dir: Path, out_dir: Path) -> RunSummary: ...
```

`import_spec` returns paths of written spec YAMLs. `ingest_year` returns
microtrade's `RunSummary` so callers can inspect `failed_count`. Keep
microtrade imports inside this module only.

## Pipeline entry point

`pipeline.py` has a `main()` that:

1. Loads `config.yaml` via pydantic-settings.
2. Plans stage 1 (dirty workbooks). Runs stage 1 if any.
3. Loads `microtrade.yaml` into pydantic models.
4. Plans stage 2 (dirty `(trade_type, year)` pairs). Runs stage 2 if any.
5. On per-year failure: log with `logger.exception` and continue with other
   years. Failed year has no raw-manifest updates, so it replans next run.

## Kubernetes deployment notes

- Single `CronJob`. One pod per run. No concurrency within a run.
- All state dirs must be on a PersistentVolume that persists across pod
  restarts.
- `microtrade.yaml` and `config.yaml` ship in the image or mount as a
  ConfigMap.
- Set `concurrencyPolicy: Forbid` on the CronJob so two runs can't race on
  the same state directory.
- Pod exit code: 0 on clean completion (including "nothing to do"), non-zero
  if any year failed. The CronJob's failure metrics then reflect real
  failures.

## Module layout

```
project/
  pyproject.toml
  config.yaml
  microtrade.yaml
  CLAUDE.md
  src/
    config.py              # pydantic-settings: load_config()
    microtrade_config.py   # pydantic models over microtrade.yaml, match_raw()
    manifest.py            # read/write manifests atomically
    hashing.py             # hash_file()
    adapter.py             # MicrotradeAdapter
    pipeline.py            # plan + run both stages, main()
  tests/
    ...
  data/
    workbooks/
    raw/
    specs/
    processed/
    manifests/
      specs/
      raw/
```

## Things this project explicitly does NOT do

- No orchestration framework.
- No long-running services.
- No remote state (database, cloud storage, API).
- No structured/JSON logging.
- No parsing of raw data files (microtrade does it).
- No schema validation of parquet output (microtrade's concern).
- No row-level routing or YTD logic (microtrade owns both).
- No retry logic beyond "next cronjob run replans dirty items".
- No per-row provenance tracking; `(trade_type, year)` is the unit of
  reprocessing.

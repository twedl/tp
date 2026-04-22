"""End-to-end tests against the real microtrade library.

Uses the example workbook and microtrade.yaml shipped with microtrade at
/Users/tweedle/projects/microtrade/examples/. Skipped if those files are
not present (e.g. in CI without the sibling repo).
"""

import shutil
from pathlib import Path

import pytest

from tp.adapter import MicrotradeAdapter
from tp.config import load_config
from tp.manifest import SpecManifest, read_manifest
from tp.pipeline import run


MICROTRADE_EXAMPLES = Path("/Users/tweedle/projects/microtrade/examples")
EXAMPLE_WORKBOOK = MICROTRADE_EXAMPLES / "microdata-layout.xls"
EXAMPLE_YAML = MICROTRADE_EXAMPLES / "microtrade.yaml"

pytestmark = pytest.mark.skipif(
    not (EXAMPLE_WORKBOOK.exists() and EXAMPLE_YAML.exists()),
    reason="microtrade example files not available",
)


@pytest.fixture
def tree(tmp_path: Path):
    for sub in (
        "workbooks",
        "raw",
        "specs",
        "processed",
        "manifests/specs",
        "manifests/raw",
    ):
        (tmp_path / sub).mkdir(parents=True)

    # Copy the real example workbook and config into the tree.
    wb_dst = tmp_path / "workbooks" / EXAMPLE_WORKBOOK.name
    shutil.copy(EXAMPLE_WORKBOOK, wb_dst)
    mt_yaml = tmp_path / "microtrade.yaml"
    shutil.copy(EXAMPLE_YAML, mt_yaml)

    cfg_yaml = tmp_path / "config.yaml"
    cfg_yaml.write_text(
        f"""
microtrade_yaml: {mt_yaml}
workbooks_dir: {tmp_path / "workbooks"}
raw_dir: {tmp_path / "raw"}
specs_dir: {tmp_path / "specs"}
processed_dir: {tmp_path / "processed"}
spec_manifests_dir: {tmp_path / "manifests" / "specs"}
raw_manifests_dir: {tmp_path / "manifests" / "raw"}
upstream_raw_dir: {tmp_path / "upstream"}
raw_remote_dir: {tmp_path / "remote"}
"""
    )
    return load_config(cfg_yaml), tmp_path


def test_import_spec_against_real_workbook(tree):
    settings, _root = tree
    adapter = MicrotradeAdapter()
    wb = next(settings.workbooks_dir.iterdir())
    specs = adapter.import_spec(wb, settings.microtrade_yaml, settings.specs_dir)

    # The example workbook declares three trade types.
    assert {p.parent.name for p in specs} == {
        "imports",
        "exports_us",
        "exports_nonus",
    }
    for p in specs:
        assert p.exists()
        assert p.name.startswith("v2020-01")


def test_full_run_stage1_only(tree):
    """End-to-end with real adapter: stage 1 succeeds, stage 2 has no raw
    files so it's a no-op. Exits 0."""
    settings, _root = tree
    assert run(settings, MicrotradeAdapter()) == 0

    wb = next(settings.workbooks_dir.iterdir())
    m = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)
    assert m is not None
    assert m.workbook_name == wb.name
    assert len(m.specs_written) == 3
    # Specs materialized on disk where microtrade expects them.
    for spec_path in m.specs_written:
        assert spec_path.exists()


def test_rerun_is_idempotent(tree):
    settings, _root = tree
    adapter = MicrotradeAdapter()

    assert run(settings, adapter) == 0
    wb = next(settings.workbooks_dir.iterdir())
    m1 = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)

    assert run(settings, adapter) == 0
    m2 = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)
    # Manifest unchanged because nothing was dirty on the second run.
    assert m1 == m2


def test_ingest_year_against_empty_raw_dir(tree):
    """Direct call into adapter.ingest_year with no raw files -> empty
    RunSummary with no failures. Exercises the real microtrade pipeline API."""
    settings, _root = tree
    adapter = MicrotradeAdapter()
    # Stage 1 must run first to produce the committed specs microtrade reads.
    wb = next(settings.workbooks_dir.iterdir())
    adapter.import_spec(wb, settings.microtrade_yaml, settings.specs_dir)

    summary = adapter.ingest_year(
        trade_type="imports",
        year=2020,
        raw_dir=settings.raw_dir,
        specs_dir=settings.specs_dir,
        out_dir=settings.processed_dir,
    )
    assert summary.failed_count == 0
    assert summary.ok_count == 0


def test_microtrade_yaml_change_retriggers_import(tree):
    settings, _root = tree
    adapter = MicrotradeAdapter()

    assert run(settings, adapter) == 0
    wb = next(settings.workbooks_dir.iterdir())
    m1 = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)

    # Append a comment to microtrade.yaml -> hash changes.
    settings.microtrade_yaml.write_text(
        settings.microtrade_yaml.read_text() + "\n# bumped\n"
    )
    assert run(settings, adapter) == 0
    m2 = read_manifest(settings.spec_manifests_dir, wb.name, SpecManifest)
    assert m1 is not None and m2 is not None
    assert m2.microtrade_hash != m1.microtrade_hash

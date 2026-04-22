from dataclasses import dataclass
from pathlib import Path

import pytest

from tp.adapter import MicrotradeAdapter
from tp.manifest import (
    RawManifest,
    SpecManifest,
    read_manifest,
)
from tp.pipeline import run


@dataclass
class FakeSummary:
    failed_count: int = 0


class FakeAdapter(MicrotradeAdapter):
    def __init__(
        self,
        *,
        ingest_fail_for: set[tuple[str, int]] | None = None,
        import_fail_for: set[str] | None = None,
    ):
        self.import_calls: list[tuple[Path, Path, Path]] = []
        self.ingest_calls: list[tuple[str, int, Path, Path, Path]] = []
        self.ingest_fail_for = ingest_fail_for or set()
        self.import_fail_for = import_fail_for or set()

    def import_spec(self, workbook, microtrade_yaml, specs_out):
        self.import_calls.append((workbook, microtrade_yaml, specs_out))
        if workbook.name in self.import_fail_for:
            raise RuntimeError(f"import boom: {workbook.name}")
        out = specs_out / f"{workbook.stem}.spec.yaml"
        out.write_text("stub\n")
        return [out]

    def ingest_year(self, trade_type, year, raw_dir, specs_dir, out_dir):
        self.ingest_calls.append((trade_type, year, raw_dir, specs_dir, out_dir))
        if (trade_type, year) in self.ingest_fail_for:
            raise RuntimeError(f"ingest boom: {(trade_type, year)}")
        year_dir = out_dir / trade_type / f"year={year}"
        (year_dir / "month=01").mkdir(parents=True, exist_ok=True)
        (year_dir / "month=01" / "part-0.parquet").write_text("stub")
        return FakeSummary(failed_count=0)


@pytest.fixture
def transport_spy(monkeypatch):
    calls: dict[str, list] = {"mirror": [], "pull": [], "push": []}
    monkeypatch.setattr(
        "tp.pipeline.mirror_upstream_raw",
        lambda s: calls["mirror"].append(s),
    )
    monkeypatch.setattr(
        "tp.pipeline.pull_raw", lambda s: calls["pull"].append(s)
    )
    monkeypatch.setattr(
        "tp.pipeline.push_processed",
        lambda s, dirs: calls["push"].append(list(dirs)),
    )
    return calls


def test_empty_run_exits_clean(tree, transport_spy):
    settings, _root = tree
    adapter = FakeAdapter()
    assert run(settings, adapter) == 0
    assert adapter.import_calls == []
    assert adapter.ingest_calls == []
    assert len(transport_spy["mirror"]) == 1
    assert len(transport_spy["pull"]) == 1
    assert transport_spy["push"] == []


def test_happy_path(tree, transport_spy):
    settings, root = tree
    wb = root / "workbooks" / "wb2020.xls"
    wb.write_bytes(b"workbook")
    raw_a = root / "raw" / "S1_202001N.TXT.zip"
    raw_a.write_bytes(b"raw-a")
    raw_b = root / "raw" / "S2_202003N.TXT.zip"
    raw_b.write_bytes(b"raw-b")

    adapter = FakeAdapter()
    assert run(settings, adapter) == 0

    assert len(adapter.import_calls) == 1
    called_wb, called_yaml, called_specs_out = adapter.import_calls[0]
    assert called_wb == wb
    assert called_yaml == settings.microtrade_yaml
    assert called_specs_out == settings.specs_dir

    assert len(adapter.ingest_calls) == 2
    keys = {(c[0], c[1]) for c in adapter.ingest_calls}
    assert keys == {("imports", 2020), ("exports_us", 2020)}
    for _tt, _y, _rd, _sd, out_dir in adapter.ingest_calls:
        assert out_dir == settings.processed_dir

    spec_m = read_manifest(settings.spec_manifests_dir, "wb2020.xls", SpecManifest)
    assert spec_m is not None

    rm_a = read_manifest(settings.raw_manifests_dir, raw_a.name, RawManifest)
    assert rm_a is not None
    assert rm_a.trade_type == "imports"
    assert rm_a.year == "2020"
    assert rm_a.month == "01"

    assert len(transport_spy["mirror"]) == 1
    assert len(transport_spy["pull"]) == 1
    assert len(transport_spy["push"]) == 2


def test_rerun_is_noop(tree, transport_spy):
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    (root / "raw" / "S1_202001N.TXT.zip").write_bytes(b"raw")

    a1 = FakeAdapter()
    assert run(settings, a1) == 0
    assert len(a1.import_calls) == 1
    assert len(a1.ingest_calls) == 1

    a2 = FakeAdapter()
    assert run(settings, a2) == 0
    assert a2.import_calls == []
    assert a2.ingest_calls == []


def test_year_failure_isolated_nonzero_exit(tree, transport_spy):
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    good = root / "raw" / "S1_202001N.TXT.zip"
    bad = root / "raw" / "S2_202003N.TXT.zip"
    good.write_bytes(b"g")
    bad.write_bytes(b"b")

    adapter = FakeAdapter(ingest_fail_for={("exports_us", 2020)})
    assert run(settings, adapter) == 1

    assert read_manifest(
        settings.raw_manifests_dir, good.name, RawManifest
    ) is not None
    assert read_manifest(
        settings.raw_manifests_dir, bad.name, RawManifest
    ) is None
    assert len(transport_spy["push"]) == 1


def test_stage1_failure_isolated_nonzero_exit(tree, transport_spy):
    settings, root = tree
    good_wb = root / "workbooks" / "good.xls"
    bad_wb = root / "workbooks" / "bad.xls"
    good_wb.write_bytes(b"g")
    bad_wb.write_bytes(b"b")

    adapter = FakeAdapter(import_fail_for={bad_wb.name})
    assert run(settings, adapter) == 1

    assert read_manifest(
        settings.spec_manifests_dir, good_wb.name, SpecManifest
    ) is not None
    assert read_manifest(
        settings.spec_manifests_dir, bad_wb.name, SpecManifest
    ) is None


def test_microtrade_failed_count_triggers_year_failure(tree, transport_spy):
    settings, root = tree
    (root / "raw" / "S1_202001N.TXT.zip").write_bytes(b"raw")

    class FailingSummaryAdapter(FakeAdapter):
        def ingest_year(self, trade_type, year, raw_dir, specs_dir, out_dir):
            super().ingest_year(trade_type, year, raw_dir, specs_dir, out_dir)
            return FakeSummary(failed_count=1)

    adapter = FailingSummaryAdapter()
    assert run(settings, adapter) == 1
    assert read_manifest(
        settings.raw_manifests_dir, "S1_202001N.TXT.zip", RawManifest
    ) is None


def test_default_adapter_raises(tree, transport_spy):
    settings, root = tree
    (root / "workbooks" / "wb2020.xls").write_bytes(b"wb")
    assert run(settings, MicrotradeAdapter()) == 1

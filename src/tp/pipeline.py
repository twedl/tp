import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from tp.adapter import MicrotradeAdapter
from tp.config import Settings, load_config
from tp.hashing import hash_file
from tp.manifest import (
    RawManifest,
    SpecManifest,
    read_manifest,
    write_manifest,
)
from tp.microtrade_config import (
    MicrotradeConfig,
    load_microtrade_config,
    match_raw,
)
from tp.transport import mirror_upstream_raw, pull_raw, push_processed


@dataclass(frozen=True)
class YearKey:
    trade_type: str
    year: int


def plan_stage1(settings: Settings) -> list[Path]:
    microtrade_hash = hash_file(settings.microtrade_yaml)
    dirty: list[Path] = []
    for wb in sorted(settings.workbooks_dir.iterdir()):
        if not wb.is_file():
            continue
        manifest = read_manifest(
            settings.spec_manifests_dir, wb.name, SpecManifest
        )
        if manifest is None:
            dirty.append(wb)
            continue
        if manifest.microtrade_hash != microtrade_hash:
            dirty.append(wb)
            continue
        if manifest.workbook_hash != hash_file(wb):
            dirty.append(wb)
            continue
    return dirty


def plan_stage2(
    settings: Settings, cfg: MicrotradeConfig
) -> dict[YearKey, list[Path]]:
    microtrade_hash = hash_file(settings.microtrade_yaml)

    years: dict[YearKey, list[Path]] = {}
    dirty_keys: set[YearKey] = set()

    for raw in sorted(settings.raw_dir.iterdir()):
        if not raw.is_file():
            continue
        m = match_raw(raw.name, cfg)
        if m is None:
            logger.warning("no matching sheet for raw file: {}", raw.name)
            continue
        key = YearKey(m.trade_type, int(m.year))
        years.setdefault(key, []).append(raw)

        # Once a year is known dirty, every raw in it will be reprocessed;
        # hashing further raws in that year is wasted I/O.
        if key in dirty_keys:
            continue

        manifest = read_manifest(
            settings.raw_manifests_dir, raw.name, RawManifest
        )
        if (
            manifest is None
            or manifest.microtrade_hash != microtrade_hash
            or manifest.raw_hash != hash_file(raw)
        ):
            dirty_keys.add(key)

    return {k: years[k] for k in dirty_keys}


def _run_stage1(settings: Settings, adapter: MicrotradeAdapter) -> int:
    dirty = plan_stage1(settings)
    if not dirty:
        logger.info("stage 1: nothing to do")
        return 0
    logger.info("stage 1: {} workbook(s) to process", len(dirty))
    mt_hash = hash_file(settings.microtrade_yaml)
    failures = 0
    for wb in dirty:
        try:
            specs = adapter.import_spec(
                wb, settings.microtrade_yaml, settings.specs_dir
            )
            manifest = SpecManifest(
                workbook_name=wb.name,
                workbook_hash=hash_file(wb),
                microtrade_hash=mt_hash,
                specs_written=specs,
                processed_at=datetime.now(tz=timezone.utc),
            )
            write_manifest(settings.spec_manifests_dir, wb.name, manifest)
        except Exception:
            logger.exception("stage 1 failed for workbook {}", wb.name)
            failures += 1
    return failures


def _year_output_dir(settings: Settings, key: YearKey) -> Path:
    return settings.processed_dir / key.trade_type / f"year={key.year}"


def _run_stage2(
    settings: Settings, cfg: MicrotradeConfig, adapter: MicrotradeAdapter
) -> int:
    dirty = plan_stage2(settings, cfg)
    if not dirty:
        logger.info("stage 2: nothing to do")
        return 0
    logger.info("stage 2: {} (trade_type, year) to process", len(dirty))
    mt_hash = hash_file(settings.microtrade_yaml)
    failures = 0
    for key, raws in dirty.items():
        try:
            summary = adapter.ingest_year(
                trade_type=key.trade_type,
                year=key.year,
                raw_dir=settings.raw_dir,
                specs_dir=settings.specs_dir,
                out_dir=settings.processed_dir,
            )
            if summary.failed_count > 0:
                raise RuntimeError(
                    f"microtrade reported {summary.failed_count} partition failure(s)"
                )

            push_processed(settings, [_year_output_dir(settings, key)])

            now = datetime.now(tz=timezone.utc)
            for raw in raws:
                m = match_raw(raw.name, cfg)
                assert m is not None
                manifest = RawManifest(
                    raw_name=raw.name,
                    raw_hash=hash_file(raw),
                    microtrade_hash=mt_hash,
                    trade_type=m.trade_type,
                    year=m.year,
                    month=m.month,
                    flag=m.flag,
                    processed_at=now,
                )
                write_manifest(settings.raw_manifests_dir, raw.name, manifest)
        except Exception:
            logger.exception("stage 2 failed for {}", key)
            failures += 1
    return failures


def run(settings: Settings, adapter: MicrotradeAdapter) -> int:
    mirror_upstream_raw(settings)
    pull_raw(settings)

    stage1_failures = _run_stage1(settings, adapter)

    cfg = load_microtrade_config(settings.microtrade_yaml)
    stage2_failures = _run_stage2(settings, cfg, adapter)

    total = stage1_failures + stage2_failures
    if total:
        logger.error("run completed with {} failure(s)", total)
        return 1
    logger.info("run completed cleanly")
    return 0


def main() -> None:
    sys.exit(run(load_config(), MicrotradeAdapter()))

from pathlib import Path

from microtrade import config as mt_config
from microtrade import excel_spec, schema
from microtrade.pipeline import PipelineConfig, RunSummary, run as mt_run


class MicrotradeAdapter:
    def import_spec(
        self,
        workbook: Path,
        microtrade_yaml: Path,
        specs_out: Path,
    ) -> list[Path]:
        project_config = mt_config.load_config(microtrade_yaml)
        workbook_config = project_config.get_workbook(workbook)
        specs = excel_spec.read_workbook(workbook, workbook_config)
        effective_from = workbook_config.effective_from
        written: list[Path] = []
        for trade_type, spec in specs.items():
            target = specs_out / trade_type / f"v{effective_from}.yaml"
            target.parent.mkdir(parents=True, exist_ok=True)
            schema.save_spec(spec, target)
            written.append(target)
        return written

    def ingest_year(
        self,
        trade_type: str,
        year: int,
        raw_dir: Path,
        specs_dir: Path,
        out_dir: Path,
    ) -> RunSummary:
        cfg = PipelineConfig(
            input_dir=raw_dir,
            output_dir=out_dir,
            spec_dir=specs_dir,
            trade_types=(trade_type,),
            year=year,
            ytd=False,
            show_progress=False,
        )
        return mt_run(cfg)

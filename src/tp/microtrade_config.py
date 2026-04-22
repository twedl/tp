import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, field_validator

_YM_RE = re.compile(r"^\d{4}-\d{2}$")


class Sheet(BaseModel):
    model_config = ConfigDict(extra="ignore")

    trade_type: str
    filename_pattern: str

    @field_validator("filename_pattern")
    @classmethod
    def _compile_pattern(cls, v: str) -> str:
        re.compile(v)
        return v


class Workbook(BaseModel):
    model_config = ConfigDict(extra="ignore")

    workbook_id: str
    effective_from: str
    effective_to: str | None = None
    sheets: dict[str, Sheet]

    @field_validator("effective_from", "effective_to", mode="before")
    @classmethod
    def _normalize_ym(cls, v: Any) -> Any:
        if v is None:
            return None
        if hasattr(v, "strftime"):
            return v.strftime("%Y-%m")
        s = str(v)
        if not _YM_RE.match(s):
            raise ValueError(f"expected YYYY-MM, got {s!r}")
        return s


class MicrotradeConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    workbooks: dict[str, Workbook]


class Match(BaseModel):
    model_config = ConfigDict(frozen=True)

    workbook_id: str
    sheet_name: str
    trade_type: str
    year: str
    month: str
    flag: str


def load_microtrade_config(path: Path) -> MicrotradeConfig:
    with open(path) as f:
        data = yaml.safe_load(f)
    return MicrotradeConfig.model_validate(data)


def match_raw(filename: str, cfg: MicrotradeConfig) -> Match | None:
    for wb in cfg.workbooks.values():
        for sheet_name, sheet in wb.sheets.items():
            m = re.match(sheet.filename_pattern, filename)
            if m is None:
                continue
            gd = m.groupdict()
            ym = f"{gd['year']}-{gd['month']}"
            if ym < wb.effective_from:
                continue
            if wb.effective_to is not None and ym > wb.effective_to:
                continue
            return Match(
                workbook_id=wb.workbook_id,
                sheet_name=sheet_name,
                trade_type=sheet.trade_type,
                year=gd["year"],
                month=gd["month"],
                flag=gd["flag"],
            )
    return None

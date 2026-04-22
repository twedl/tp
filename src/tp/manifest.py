import os
from datetime import datetime
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel, ConfigDict


class SpecManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workbook_name: str
    workbook_hash: str
    microtrade_hash: str
    specs_written: list[Path]
    processed_at: datetime


class RawManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_name: str
    raw_hash: str
    microtrade_hash: str
    trade_type: str
    year: str
    month: str
    flag: str
    processed_at: datetime


M = TypeVar("M", bound=BaseModel)


def _manifest_path(directory: Path, key: str) -> Path:
    return directory / f"{key}.json"


def read_manifest(directory: Path, key: str, model: type[M]) -> M | None:
    p = _manifest_path(directory, key)
    try:
        return model.model_validate_json(p.read_text())
    except FileNotFoundError:
        return None


def write_manifest(directory: Path, key: str, manifest: BaseModel) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    p = _manifest_path(directory, key)
    tmp = directory / f"{p.name}.tmp"
    tmp.write_text(manifest.model_dump_json(indent=2))
    os.replace(tmp, p)

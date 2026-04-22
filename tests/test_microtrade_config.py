from pathlib import Path

import pytest

from tp.microtrade_config import load_microtrade_config, match_raw


FIXTURE_YAML = """
workbooks:
  microdata-layout.xls:
    workbook_id: MICRODATA2020
    effective_from: 2020-01
    effective_to: 2023-12
    sheets:
      SHEETNAME001:
        trade_type: imports
        filename_pattern: '^SHEETNAME001_(?P<year>\\d{4})(?P<month>\\d{2})(?P<flag>[NC])\\.TXT\\.zip$'
        routing_column: year_month
      SHEETNAME002:
        trade_type: exports_us
        filename_pattern: '^SHEETNAME002_(?P<year>\\d{4})(?P<month>\\d{2})(?P<flag>[NC])\\.TXT\\.zip$'
  microdata-layout-2024.xls:
    workbook_id: MICRODATA2024
    effective_from: 2024-01
    sheets:
      SHEETNAME001:
        trade_type: imports
        filename_pattern: '^SHEETNAME001_(?P<year>\\d{4})(?P<month>\\d{2})(?P<flag>[NC])\\.TXT\\.zip$'
"""


@pytest.fixture
def cfg(tmp_path: Path):
    p = tmp_path / "microtrade.yaml"
    p.write_text(FIXTURE_YAML)
    return load_microtrade_config(p)


def test_happy_match(cfg):
    m = match_raw("SHEETNAME001_202001N.TXT.zip", cfg)
    assert m is not None
    assert m.workbook_id == "MICRODATA2020"
    assert m.sheet_name == "SHEETNAME001"
    assert m.trade_type == "imports"
    assert m.year == "2020"
    assert m.month == "01"
    assert m.flag == "N"


def test_no_match(cfg):
    assert match_raw("random_file.txt", cfg) is None
    assert match_raw("SHEETNAME001_202001X.TXT.zip", cfg) is None


def test_first_match_wins_across_workbooks(cfg):
    # 2020-06 falls in 2020 workbook window -> that's the match.
    m = match_raw("SHEETNAME001_202006N.TXT.zip", cfg)
    assert m is not None
    assert m.workbook_id == "MICRODATA2020"


def test_date_outside_2020_falls_through_to_2024(cfg):
    # 2025-03 is past 2020 window (2023-12), inside 2024 window (open).
    m = match_raw("SHEETNAME001_202503N.TXT.zip", cfg)
    assert m is not None
    assert m.workbook_id == "MICRODATA2024"


def test_date_outside_all_windows_no_match(cfg):
    # 2019 is before 2020 workbook's effective_from.
    assert match_raw("SHEETNAME001_201912N.TXT.zip", cfg) is None


def test_open_ended_effective_to(cfg):
    # 2099 should still match 2024 workbook (no effective_to).
    m = match_raw("SHEETNAME001_209912N.TXT.zip", cfg)
    assert m is not None
    assert m.workbook_id == "MICRODATA2024"


def test_window_boundaries_inclusive(cfg):
    assert match_raw("SHEETNAME001_202001N.TXT.zip", cfg).workbook_id == "MICRODATA2020"
    assert match_raw("SHEETNAME001_202312N.TXT.zip", cfg).workbook_id == "MICRODATA2020"
    assert match_raw("SHEETNAME001_202401N.TXT.zip", cfg).workbook_id == "MICRODATA2024"


def test_sheet_only_in_2020_does_not_leak(cfg):
    # SHEETNAME002 exists only in 2020 workbook; 2025 date => no match.
    assert match_raw("SHEETNAME002_202503N.TXT.zip", cfg) is None


def test_invalid_regex_fails_load(tmp_path: Path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "workbooks:\n"
        "  wb.xls:\n"
        "    workbook_id: WB\n"
        "    effective_from: 2020-01\n"
        "    sheets:\n"
        "      S:\n"
        "        trade_type: imports\n"
        "        filename_pattern: '['\n"
    )
    with pytest.raises(Exception):
        load_microtrade_config(bad)


def test_invalid_effective_from_fails_load(tmp_path: Path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "workbooks:\n"
        "  wb.xls:\n"
        "    workbook_id: WB\n"
        "    effective_from: '2020'\n"
        "    sheets: {}\n"
    )
    with pytest.raises(Exception):
        load_microtrade_config(bad)

from pathlib import Path

import pytest

from survey_automation.config import ConfigError, load_config


def _write_config(path: Path, max_warning_count_value: str) -> None:
    path.write_text(
        "version: '1'\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*'\n"
        "  exclude_globs: []\n"
        "validation:\n"
        "  required_point_columns:\n"
        "    - 'Point#'\n"
        "    - 'Northing'\n"
        "    - 'Easting'\n"
        "    - 'Elevation'\n"
        "    - 'Description'\n"
        "    - 'DWG Description'\n"
        "    - 'DWG Layer'\n"
        "    - 'Locked'\n"
        "    - 'Group'\n"
        "    - 'Category'\n"
        "    - 'LS Number'\n"
        "  max_warning_count: "
        f"{max_warning_count_value}\n",
        encoding="utf-8",
    )


def test_max_warning_count_accepts_null_and_integer(tmp_path: Path) -> None:
    cfg_null = tmp_path / "null.yaml"
    _write_config(cfg_null, "null")
    loaded_null = load_config(cfg_null)
    assert loaded_null["validation"]["max_warning_count"] is None

    cfg_int = tmp_path / "int.yaml"
    _write_config(cfg_int, "10")
    loaded_int = load_config(cfg_int)
    assert loaded_int["validation"]["max_warning_count"] == 10


def test_max_warning_count_rejects_negative_and_non_int(tmp_path: Path) -> None:
    cfg_negative = tmp_path / "negative.yaml"
    _write_config(cfg_negative, "-1")
    with pytest.raises(ConfigError):
        load_config(cfg_negative)

    cfg_string = tmp_path / "string.yaml"
    _write_config(cfg_string, "'many'")
    with pytest.raises(ConfigError):
        load_config(cfg_string)


def test_required_field_code_columns_must_include_standard_headers(tmp_path: Path) -> None:
    cfg_missing = tmp_path / "missing-field-columns.yaml"
    cfg_missing.write_text(
        "version: '1'\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*'\n"
        "  exclude_globs: []\n"
        "validation:\n"
        "  required_point_columns:\n"
        "    - 'Point#'\n"
        "    - 'Northing'\n"
        "    - 'Easting'\n"
        "    - 'Elevation'\n"
        "    - 'Description'\n"
        "    - 'DWG Description'\n"
        "    - 'DWG Layer'\n"
        "    - 'Locked'\n"
        "    - 'Group'\n"
        "    - 'Category'\n"
        "    - 'LS Number'\n"
        "  required_field_code_columns:\n"
        "    - 'Field Code'\n"
        "    - 'Layer'\n"
        "    - 'Symbol'\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg_missing)


def test_required_point_columns_reject_duplicate_headers(tmp_path: Path) -> None:
    cfg_duplicate = tmp_path / "duplicate-point-columns.yaml"
    cfg_duplicate.write_text(
        "version: '1'\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*'\n"
        "  exclude_globs: []\n"
        "validation:\n"
        "  required_point_columns:\n"
        "    - 'Point#'\n"
        "    - 'Point#'\n"
        "    - 'Easting'\n"
        "    - 'Elevation'\n"
        "    - 'Description'\n"
        "    - 'DWG Description'\n"
        "    - 'DWG Layer'\n"
        "    - 'Locked'\n"
        "    - 'Group'\n"
        "    - 'Category'\n"
        "    - 'LS Number'\n"
        "  required_field_code_columns:\n"
        "    - 'Field Code'\n"
        "    - 'Layer'\n"
        "    - 'Symbol'\n"
        "    - 'Linework'\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg_duplicate)


def test_duplicate_point_id_mode_accepts_known_values(tmp_path: Path) -> None:
    cfg = tmp_path / "duplicate-mode.yaml"
    cfg.write_text(
        "version: '1'\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*'\n"
        "  exclude_globs: []\n"
        "validation:\n"
        "  duplicate_point_id_mode: per_point_id\n",
        encoding="utf-8",
    )
    loaded = load_config(cfg)
    assert loaded["validation"]["duplicate_point_id_mode"] == "per_point_id"

    cfg2 = tmp_path / "duplicate-mode-within-file.yaml"
    cfg2.write_text(
        "version: '1'\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*'\n"
        "  exclude_globs: []\n"
        "validation:\n"
        "  duplicate_point_id_mode: within_file\n",
        encoding="utf-8",
    )
    loaded2 = load_config(cfg2)
    assert loaded2["validation"]["duplicate_point_id_mode"] == "within_file"


def test_duplicate_point_id_mode_rejects_unknown_value(tmp_path: Path) -> None:
    cfg = tmp_path / "duplicate-mode-invalid.yaml"
    cfg.write_text(
        "version: '1'\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*'\n"
        "  exclude_globs: []\n"
        "validation:\n"
        "  duplicate_point_id_mode: noisy\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg)


def test_unmapped_description_skip_categories_must_be_list_of_strings(tmp_path: Path) -> None:
    cfg_bad = tmp_path / "skip-categories-invalid.yaml"
    cfg_bad.write_text(
        "version: '1'\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*'\n"
        "  exclude_globs: []\n"
        "validation:\n"
        "  unmapped_description_skip_categories: converted\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg_bad)

    cfg_ok = tmp_path / "skip-categories-valid.yaml"
    cfg_ok.write_text(
        "version: '1'\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*'\n"
        "  exclude_globs: []\n"
        "validation:\n"
        "  unmapped_description_skip_categories:\n"
        "    - converted\n"
        "    - legacy\n",
        encoding="utf-8",
    )
    loaded = load_config(cfg_ok)
    assert loaded["validation"]["unmapped_description_skip_categories"] == ["converted", "legacy"]

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


def test_qc_profile_standard_applies_profile_defaults(tmp_path: Path) -> None:
    cfg = tmp_path / "profile-standard.yaml"
    cfg.write_text(
        "version: '1'\n"
        "project:\n"
        "  qc_profile: standard\n"
        "input:\n"
        "  include_globs:\n"
        "    - '**/*'\n",
        encoding="utf-8",
    )
    loaded = load_config(cfg)
    assert loaded["project"]["qc_profile"] == "standard"
    assert "TOTaLi/IIII.dxf" in loaded["input"]["exclude_globs"]
    assert loaded["validation"]["duplicate_point_id_mode"] == "within_file"
    assert loaded["remediation"]["enabled"] is True
    assert loaded["remediation"]["drop_duplicate_tail_blocks"] is False
    assert loaded["remediation"]["drop_malformed_footer_rows"] is False


def test_qc_profile_rejects_unknown_value(tmp_path: Path) -> None:
    cfg = tmp_path / "profile-invalid.yaml"
    cfg.write_text(
        "version: '1'\n"
        "project:\n"
        "  qc_profile: unsupported\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg)


def test_trend_tracking_thresholds_require_non_negative_integers(tmp_path: Path) -> None:
    cfg = tmp_path / "trend-invalid.yaml"
    cfg.write_text(
        "version: '1'\n"
        "validation:\n"
        "  trend_tracking:\n"
        "    max_warning_delta: -1\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg)


def test_remediation_flags_require_booleans(tmp_path: Path) -> None:
    cfg = tmp_path / "remediation-invalid.yaml"
    cfg.write_text(
        "version: '1'\n"
        "remediation:\n"
        "  enabled: 'yes'\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg)


def test_project_baseline_namespace_accepts_string_and_trims(tmp_path: Path) -> None:
    cfg = tmp_path / "baseline-namespace.yaml"
    cfg.write_text(
        "version: '1'\n"
        "project:\n"
        "  baseline_namespace: '  prod-main  '\n",
        encoding="utf-8",
    )
    loaded = load_config(cfg)
    assert loaded["project"]["baseline_namespace"] == "prod-main"


def test_project_baseline_namespace_rejects_non_string(tmp_path: Path) -> None:
    cfg = tmp_path / "baseline-namespace-invalid.yaml"
    cfg.write_text(
        "version: '1'\n"
        "project:\n"
        "  baseline_namespace: 123\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg)


def test_crd_converter_failure_mode_accepts_known_values(tmp_path: Path) -> None:
    cfg = tmp_path / "converter-failure-mode-ok.yaml"
    cfg.write_text(
        "version: '1'\n"
        "crd:\n"
        "  converter_failure_mode: quarantine\n",
        encoding="utf-8",
    )
    loaded = load_config(cfg)
    assert loaded["crd"]["converter_failure_mode"] == "quarantine"


def test_crd_converter_failure_mode_rejects_unknown_value(tmp_path: Path) -> None:
    cfg = tmp_path / "converter-failure-mode-bad.yaml"
    cfg.write_text(
        "version: '1'\n"
        "crd:\n"
        "  converter_failure_mode: soft\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg)


def test_presentation_defaults_are_available(tmp_path: Path) -> None:
    cfg = tmp_path / "presentation-defaults.yaml"
    cfg.write_text("version: '1'\n", encoding="utf-8")
    loaded = load_config(cfg)
    assert loaded["presentation"]["enabled"] is True
    assert loaded["presentation"]["color_basis"] == "category_config"
    assert loaded["presentation"]["category_colors"]["config"] == "#0072B2"
    assert loaded["presentation"]["config_colors"]["qc_profile"]["standard"] == "#56B4E9"


def test_presentation_partial_overrides_merge_with_defaults(tmp_path: Path) -> None:
    cfg = tmp_path / "presentation-overrides.yaml"
    cfg.write_text(
        "version: '1'\n"
        "presentation:\n"
        "  category_colors:\n"
        "    data: '#112233'\n"
        "  config_colors:\n"
        "    crd_mode:\n"
        "      text_only: '#123456'\n",
        encoding="utf-8",
    )
    loaded = load_config(cfg)
    assert loaded["presentation"]["category_colors"]["data"] == "#112233"
    assert loaded["presentation"]["category_colors"]["config"] == "#0072B2"
    assert loaded["presentation"]["config_colors"]["crd_mode"]["text_only"] == "#123456"
    assert loaded["presentation"]["config_colors"]["crd_mode"]["auto"] == "#009E73"


def test_presentation_rejects_invalid_hex_color(tmp_path: Path) -> None:
    cfg = tmp_path / "presentation-invalid-hex.yaml"
    cfg.write_text(
        "version: '1'\n"
        "presentation:\n"
        "  category_colors:\n"
        "    config: '#12ZZ34'\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg)


def test_presentation_rejects_unknown_color_keys(tmp_path: Path) -> None:
    cfg = tmp_path / "presentation-unknown-key.yaml"
    cfg.write_text(
        "version: '1'\n"
        "presentation:\n"
        "  category_colors:\n"
        "    custom: '#123456'\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(cfg)

from survey_automation.converter import run_converter_smoke_check, run_static_converter_checks


def _checks_by_name(checks):
    return {check.name: check for check in checks}


def test_static_checks_expand_env_var_command_template() -> None:
    resolved, checks = run_static_converter_checks(
        "${CRD_CONVERTER_COMMAND}",
        env={"CRD_CONVERTER_COMMAND": "/bin/echo {input} {output}"},
    )
    by_name = _checks_by_name(checks)

    assert resolved == "/bin/echo {input} {output}"
    assert by_name["converter_command_present"].ok
    assert by_name["unresolved_env_vars"].ok
    assert by_name["required_placeholders"].ok
    assert by_name["disallow_example_converter"].ok
    assert by_name["executable_resolvable"].ok


def test_static_checks_fail_when_env_var_is_missing() -> None:
    _, checks = run_static_converter_checks("${CRD_CONVERTER_COMMAND}", env={})
    by_name = _checks_by_name(checks)

    assert not by_name["unresolved_env_vars"].ok
    assert "CRD_CONVERTER_COMMAND" in by_name["unresolved_env_vars"].message


def test_static_checks_fail_when_placeholders_are_missing() -> None:
    _, checks = run_static_converter_checks("/bin/echo converter", env={})
    by_name = _checks_by_name(checks)

    assert not by_name["required_placeholders"].ok


def test_static_checks_fail_when_example_converter_is_used() -> None:
    _, checks = run_static_converter_checks("scripts/crd_converter_example.sh {input} {output}", env={})
    by_name = _checks_by_name(checks)

    assert not by_name["disallow_example_converter"].ok


def test_static_checks_fail_when_executable_is_unresolvable() -> None:
    _, checks = run_static_converter_checks("missing_converter_binary {input} {output}", env={})
    by_name = _checks_by_name(checks)

    assert not by_name["executable_resolvable"].ok


def test_smoke_check_uses_provided_env_for_command_expansion(tmp_path) -> None:
    converter = tmp_path / "converter.sh"
    converter.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "out=\"$2\"\n"
        "cat > \"$out\" <<'CSV'\n"
        "Point#,Northing,Easting,Elevation,Description,DWG Description,DWG Layer,Locked,Group,Category,LS Number\n"
        "1001,1,2,3,CP,CP,PNTS,No,,Converted,\n"
        "CSV\n",
        encoding="utf-8",
    )
    converter.chmod(0o755)

    sample_crd = tmp_path / "sample.crd"
    sample_crd.write_bytes(b"\x00\x00New CRD Format2\x00\x00")

    check = run_converter_smoke_check(
        "${MY_CONVERTER} {input} {output}",
        sample_crd,
        env={"MY_CONVERTER": str(converter)},
    )
    assert check.ok is True

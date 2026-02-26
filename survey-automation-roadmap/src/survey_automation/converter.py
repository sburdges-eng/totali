from __future__ import annotations

import csv
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping

POINT_HEADER_PREFIX = ["Point#", "Northing", "Easting", "Elevation"]
_ENV_REF_RE = re.compile(r"\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))")
_ASSIGNMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$")


@dataclass(slots=True)
class ConverterCheck:
    name: str
    ok: bool
    message: str

    def to_dict(self) -> dict[str, str | bool]:
        return asdict(self)


def expand_command_template(command_template: str, env: Mapping[str, str] | None = None) -> str:
    source_env = env if env is not None else os.environ

    def replace(match: re.Match[str]) -> str:
        var_name = match.group(1) or match.group(2) or ""
        return source_env.get(var_name, match.group(0))

    return _ENV_REF_RE.sub(replace, command_template)


def find_unresolved_env_vars(command_template: str) -> list[str]:
    unresolved: set[str] = set()
    for match in _ENV_REF_RE.finditer(command_template):
        var_name = match.group(1) or match.group(2)
        if var_name:
            unresolved.add(var_name)
    return sorted(unresolved)


def _build_subprocess_env(env: Mapping[str, str] | None) -> dict[str, str] | None:
    if env is None:
        return None
    merged_env = dict(os.environ)
    merged_env.update(env)
    return merged_env


def run_converter_command(
    command_template: str,
    input_path: Path,
    output_path: Path,
    env: Mapping[str, str] | None = None,
) -> tuple[bool, str]:
    expanded_template = expand_command_template(command_template, env=env)
    try:
        command = expanded_template.format(
            input=shlex.quote(str(input_path)),
            output=shlex.quote(str(output_path)),
        )
    except KeyError as exc:
        return False, f"converter_command missing placeholder: {exc}"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        command,
        shell=True,
        text=True,
        capture_output=True,
        env=_build_subprocess_env(env),
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip() or f"exit code {completed.returncode}"
        return False, stderr

    if not output_path.exists():
        return False, "converter succeeded but output file was not produced"

    return True, ""


def _check_executable(command_template: str) -> ConverterCheck:
    try:
        tokens = shlex.split(command_template)
    except ValueError as exc:
        return ConverterCheck(
            name="executable_resolvable",
            ok=False,
            message=f"converter command has invalid shell syntax: {exc}",
        )

    executable: str | None = None
    for token in tokens:
        if _ASSIGNMENT_RE.fullmatch(token):
            continue
        executable = token
        break

    if not executable:
        return ConverterCheck(
            name="executable_resolvable",
            ok=False,
            message="converter command did not include an executable token",
        )

    if "/" in executable:
        executable_path = Path(executable).expanduser()
        if not executable_path.is_absolute():
            executable_path = (Path.cwd() / executable_path).resolve()
        if not executable_path.exists():
            return ConverterCheck(
                name="executable_resolvable",
                ok=False,
                message=f"converter executable path does not exist: {executable_path}",
            )
        if not os.access(executable_path, os.X_OK):
            return ConverterCheck(
                name="executable_resolvable",
                ok=False,
                message=f"converter executable is not executable: {executable_path}",
            )
        return ConverterCheck(
            name="executable_resolvable",
            ok=True,
            message=f"converter executable path is executable: {executable_path}",
        )

    resolved = shutil.which(executable)
    if not resolved:
        return ConverterCheck(
            name="executable_resolvable",
            ok=False,
            message=f"converter executable not found on PATH: {executable}",
        )
    return ConverterCheck(
        name="executable_resolvable",
        ok=True,
        message=f"converter executable resolved on PATH: {resolved}",
    )


def run_static_converter_checks(command_template: str | None, env: Mapping[str, str] | None = None) -> tuple[str, list[ConverterCheck]]:
    checks: list[ConverterCheck] = []

    if command_template is None or not command_template.strip():
        checks.append(
            ConverterCheck(
                name="converter_command_present",
                ok=False,
                message="`crd.converter_command` is missing or empty",
            )
        )
        checks.append(
            ConverterCheck(
                name="unresolved_env_vars",
                ok=False,
                message="converter command cannot be validated because it is missing",
            )
        )
        checks.append(
            ConverterCheck(
                name="required_placeholders",
                ok=False,
                message="converter command cannot be validated because it is missing",
            )
        )
        checks.append(
            ConverterCheck(
                name="disallow_example_converter",
                ok=False,
                message="converter command cannot be validated because it is missing",
            )
        )
        checks.append(
            ConverterCheck(
                name="executable_resolvable",
                ok=False,
                message="converter command cannot be validated because it is missing",
            )
        )
        return "", checks

    checks.append(
        ConverterCheck(
            name="converter_command_present",
            ok=True,
            message="converter command is configured",
        )
    )

    expanded_command = expand_command_template(command_template.strip(), env=env).strip()
    unresolved_vars = find_unresolved_env_vars(expanded_command)
    checks.append(
        ConverterCheck(
            name="unresolved_env_vars",
            ok=not unresolved_vars,
            message="no unresolved environment variables found"
            if not unresolved_vars
            else f"unresolved environment variables in converter command: {', '.join(unresolved_vars)}",
        )
    )

    has_placeholders = "{input}" in expanded_command and "{output}" in expanded_command
    checks.append(
        ConverterCheck(
            name="required_placeholders",
            ok=has_placeholders,
            message="converter command includes `{input}` and `{output}` placeholders"
            if has_placeholders
            else "converter command must include both `{input}` and `{output}` placeholders",
        )
    )

    uses_example = "crd_converter_example.sh" in expanded_command
    checks.append(
        ConverterCheck(
            name="disallow_example_converter",
            ok=not uses_example,
            message="converter command does not use example converter script"
            if not uses_example
            else "converter command still references `crd_converter_example.sh`",
        )
    )

    checks.append(_check_executable(expanded_command))
    return expanded_command, checks


def _validate_point_csv_header(path: Path) -> tuple[bool, str]:
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if row and any(cell.strip() for cell in row):
                header = [cell.strip() for cell in row]
                if len(header) < 4:
                    return False, "converted output header has fewer than four columns"
                if header[:4] != POINT_HEADER_PREFIX:
                    return (
                        False,
                        "converted output is not a supported point CSV header; expected "
                        f"{','.join(POINT_HEADER_PREFIX)} as the first columns",
                    )
                return True, "converted output has a valid point CSV header"
    return False, "converted output was empty"


def run_converter_smoke_check(
    command_template: str,
    sample_crd_path: Path,
    env: Mapping[str, str] | None = None,
) -> ConverterCheck:
    if not sample_crd_path.exists():
        return ConverterCheck(
            name="smoke_conversion",
            ok=False,
            message=f"sample CRD file not found: {sample_crd_path}",
        )
    if not sample_crd_path.is_file():
        return ConverterCheck(
            name="smoke_conversion",
            ok=False,
            message=f"sample CRD path is not a file: {sample_crd_path}",
        )

    with tempfile.TemporaryDirectory(prefix="survey-automation-crd-smoke-") as tmp_dir:
        output_path = Path(tmp_dir) / "converted.csv"
        ok, message = run_converter_command(
            command_template=command_template,
            input_path=sample_crd_path.resolve(),
            output_path=output_path,
            env=env,
        )
        if not ok:
            return ConverterCheck(
                name="smoke_conversion",
                ok=False,
                message=f"converter smoke run failed: {message}",
            )

        header_ok, header_message = _validate_point_csv_header(output_path)
        return ConverterCheck(
            name="smoke_conversion",
            ok=header_ok,
            message=header_message,
        )

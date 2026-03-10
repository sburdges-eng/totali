import sys

filename = "groundtruthos-data/survey-automation/src/survey_automation/detection.py"
with open(filename, "r") as f:
    content = f.read()

old_code = """def discover_files(
    input_dir: Path,
    include_globs: list[str],
    exclude_globs: list[str],
) -> list[Path]:
    included_by_real_path: dict[Path, Path] = {}
    for pattern in include_globs:
        for candidate in input_dir.glob(pattern):
            if candidate.is_file():
                resolved = candidate.resolve()
                existing = included_by_real_path.get(resolved)
                if existing is None or candidate.as_posix() < existing.as_posix():
                    included_by_real_path[resolved] = candidate

    excluded_real_paths: set[Path] = set()
    for pattern in exclude_globs:
        for candidate in input_dir.glob(pattern):
            if candidate.is_file():
                excluded_real_paths.add(candidate.resolve())

    filtered = [
        path
        for resolved, path in included_by_real_path.items()
        if resolved not in excluded_real_paths
    ]
    filtered.sort(key=lambda p: p.as_posix())
    return filtered"""

new_code = """def discover_files(
    input_dir: Path,
    include_globs: list[str],
    exclude_globs: list[str],
) -> list[Path]:
    unique_includes = list(dict.fromkeys(include_globs))
    unique_excludes = list(dict.fromkeys(exclude_globs))

    seen_candidates: dict[str, tuple[bool, Path | None]] = {}

    included_by_real_path: dict[Path, Path] = {}
    for pattern in unique_includes:
        for candidate in input_dir.glob(pattern):
            cand_posix = candidate.as_posix()

            if cand_posix in seen_candidates:
                is_f, resolved = seen_candidates[cand_posix]
            else:
                is_f = candidate.is_file()
                resolved = candidate.resolve() if is_f else None
                seen_candidates[cand_posix] = (is_f, resolved)

            if is_f and resolved is not None:
                existing = included_by_real_path.get(resolved)
                if existing is None or cand_posix < existing.as_posix():
                    included_by_real_path[resolved] = candidate

    excluded_real_paths: set[Path] = set()
    for pattern in unique_excludes:
        for candidate in input_dir.glob(pattern):
            cand_posix = candidate.as_posix()

            if cand_posix in seen_candidates:
                is_f, resolved = seen_candidates[cand_posix]
            else:
                is_f = candidate.is_file()
                resolved = candidate.resolve() if is_f else None
                seen_candidates[cand_posix] = (is_f, resolved)

            if is_f and resolved is not None:
                excluded_real_paths.add(resolved)

    filtered = [
        path
        for resolved, path in included_by_real_path.items()
        if resolved not in excluded_real_paths
    ]
    filtered.sort(key=lambda p: p.as_posix())
    return filtered"""

if old_code in content:
    content = content.replace(old_code, new_code)
    with open(filename, "w") as f:
        f.write(content)
    print("Patch applied successfully.")
else:
    print("Could not find the old code to patch.")
    sys.exit(1)

import time
import tempfile
from pathlib import Path
import fnmatch
import os

def run_benchmark():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        # Create a large number of dummy files and directories
        num_dirs = 50
        num_files_per_dir = 500

        for i in range(num_dirs):
            dir_path = tmp_path / f"dir_{i}"
            dir_path.mkdir()
            for j in range(num_files_per_dir):
                file_path = dir_path / f"file_{j}.csv"
                file_path.touch()
                file_path = dir_path / f"file_{j}.txt"
                file_path.touch()

        # Some generic globs + some very specific ones
        include_globs = ["**/*.csv", "**/*.txt", "**/*_0.csv", "dir_10/*.csv", "dir_20/*.csv"] * 10
        exclude_globs = ["**/*_5.csv", "**/*_15.csv"] * 5

        def discover_files_orig(
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
            return filtered

        start_time = time.perf_counter()
        res1 = discover_files_orig(tmp_path, include_globs, exclude_globs)
        end_time = time.perf_counter()
        print(f"Time taken (original): {end_time - start_time:.4f} seconds")

        def discover_files_new(
            input_dir: Path,
            include_globs: list[str],
            exclude_globs: list[str],
        ) -> list[Path]:
            # Consolidate patterns by eliminating duplicates, while preserving order
            unique_includes = list(dict.fromkeys(include_globs))
            unique_excludes = list(dict.fromkeys(exclude_globs))

            # Cache resolved paths and `is_file()` checks to avoid redundant disk I/O
            # since different globs might match the same file.
            seen_candidates: dict[str, tuple[bool, Path]] = {}

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
            return filtered

        start_time = time.perf_counter()
        res2 = discover_files_new(tmp_path, include_globs, exclude_globs)
        end_time = time.perf_counter()
        print(f"Time taken (new): {end_time - start_time:.4f} seconds")

        print("Diff res1 vs res2", len(res1), len(res2))

if __name__ == "__main__":
    run_benchmark()

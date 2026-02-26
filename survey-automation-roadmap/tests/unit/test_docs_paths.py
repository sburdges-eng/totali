import re
from pathlib import Path

DOC_FILES = [
    "README.md",
    "docs/crd-converter.md",
    "docs/operations.md",
    "docs/roadmap-pt2.md",
    "docs/release-candidate-checklist.md",
    "docs/release-notes-v2.0.0.md",
]

PATH_PREFIXES = (
    ".github/",
    "config/",
    "docs/",
    "samples/",
    "scripts/",
    "src/",
    "tests/",
    "validation/",
)

CHECKABLE_EXTENSIONS = {
    "",
    ".csv",
    ".crd",
    ".dxf",
    ".md",
    ".py",
    ".sh",
    ".toml",
    ".yaml",
    ".yml",
}


def _extract_doc_paths(markdown: str) -> set[str]:
    candidates: set[str] = set()
    for token in re.findall(r"`([^`\n]+)`", markdown):
        candidate = token.strip().split()[0]
        if not candidate.startswith(PATH_PREFIXES):
            continue
        if any(char in candidate for char in "<>{}$*"):
            continue
        suffix = Path(candidate).suffix.lower()
        if suffix not in CHECKABLE_EXTENSIONS:
            continue
        candidates.add(candidate)
    return candidates


def test_referenced_doc_paths_exist(repo_root: Path) -> None:
    missing: list[str] = []
    for relative_doc in DOC_FILES:
        doc_path = repo_root / relative_doc
        assert doc_path.exists(), f"Documentation file is missing: {relative_doc}"
        for reference in sorted(_extract_doc_paths(doc_path.read_text(encoding="utf-8"))):
            if not (repo_root / reference).exists():
                missing.append(f"{relative_doc}: `{reference}`")

    assert not missing, "Missing referenced paths:\n" + "\n".join(missing)

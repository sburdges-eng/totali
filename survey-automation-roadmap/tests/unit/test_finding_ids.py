from survey_automation.models import QCFinding
from survey_automation.pipeline import _finalize_findings


def _finding(
    *,
    severity: str,
    code: str,
    file_path: str,
    row_number: int | None,
    message: str,
) -> QCFinding:
    return QCFinding(
        finding_id="",
        severity=severity,
        code=code,
        message=message,
        file_path=file_path,
        row_number=row_number,
        run_id="run-test",
    )


def test_finalize_findings_assigns_sorted_ids_without_input_mutation() -> None:
    findings = [
        _finding(
            severity="warning",
            code="z_warning",
            file_path="b.csv",
            row_number=5,
            message="warning last",
        ),
        _finding(
            severity="error",
            code="a_error",
            file_path="a.csv",
            row_number=None,
            message="error first",
        ),
        _finding(
            severity="warning",
            code="a_warning",
            file_path="a.csv",
            row_number=2,
            message="warning middle",
        ),
    ]

    finalized = _finalize_findings("run-abc", findings)

    # The finalizer creates new records so callers cannot observe in-place mutation.
    assert all(original.finding_id == "" for original in findings)
    assert not any(original is updated for original in findings for updated in finalized)

    assert [(item.severity, item.code) for item in finalized] == [
        ("error", "a_error"),
        ("warning", "a_warning"),
        ("warning", "z_warning"),
    ]
    assert [item.finding_id for item in finalized] == [
        "run-abc-F000001",
        "run-abc-F000002",
        "run-abc-F000003",
    ]

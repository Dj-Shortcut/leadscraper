from src.cli import score_record


def test_score_record_recent_date_adds_30() -> None:
    score, reasons = score_record(
        age_months=3,
        sector_bucket="other",
        has_nace=True,
        max_months=18,
    )

    assert score == 30
    assert "new<18m" in reasons.split("|")


def test_score_record_missing_nace_minus_5() -> None:
    score, reasons = score_record(
        age_months=3,
        sector_bucket="other",
        has_nace=False,
        max_months=18,
    )

    assert score == 25
    assert "no_nace" in reasons.split("|")

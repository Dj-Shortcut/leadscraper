from src.cli import score_record


def test_score_record_recent_date_adds_30() -> None:
    score, reasons = score_record(
        status="INACTIVE",
        age_months=3,
        sector_bucket="other",
        has_nace=True,
        max_months=18,
    )

    assert score == 30
    assert "new<18m;+30" in reasons.split("|")

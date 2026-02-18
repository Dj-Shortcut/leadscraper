from src.cli import score_record


def test_score_record_recent_date_adds_30() -> None:
    score, reasons = score_record(
        age_months=3,
        sector_bucket="other",
        has_nace=True,
        has_phone=False,
        has_email=False,
        max_months=18,
    )

    assert score == 30
    assert "new<18m" in reasons.split("|")


def test_score_record_missing_nace_minus_5() -> None:
    score, reasons = score_record(
        age_months=3,
        sector_bucket="other",
        has_nace=False,
        has_phone=False,
        has_email=False,
        max_months=18,
    )

    assert score == 25
    assert "no_nace" in reasons.split("|")


def test_score_record_contact_points_added() -> None:
    score, reasons = score_record(
        age_months=3,
        sector_bucket="other",
        has_nace=True,
        has_phone=True,
        has_email=True,
        max_months=18,
    )

    assert score == 38
    assert "has_phone" in reasons.split("|")
    assert "has_email" in reasons.split("|")

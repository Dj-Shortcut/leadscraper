from src.transform import bucket_from_nace


def test_bucket_from_nace_mapping() -> None:
    assert bucket_from_nace("96.02") == "beauty"
    assert bucket_from_nace("56101") == "horeca"
    assert bucket_from_nace("86210") == "health"
    assert bucket_from_nace("47240") == "retail"
    assert bucket_from_nace("43210") == "service_trades"
    assert bucket_from_nace(None) == "other"

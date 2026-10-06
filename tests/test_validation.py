import pytest

from pipeline.validation import (
    ValidationError,
    Validator,
    is_decimal,
    max_length,
    non_negative,
    one_of,
    required,
)


class TestChecks:
    def test_required_rejects_none_and_blank(self):
        assert required(None) is not None
        assert required("") is not None
        assert required("   ") is not None
        assert required("value") is None
        assert required(0) is None  # zero is a value, not a blank

    def test_is_decimal(self):
        assert is_decimal("12.34") is None
        assert is_decimal(42) is None
        assert is_decimal(None) is None  # emptiness is `required`'s business
        assert is_decimal("not a number") is not None

    def test_non_negative(self):
        assert non_negative("5") is None
        assert non_negative("0") is None
        assert non_negative("-1") is not None

    def test_non_negative_defers_type_errors_to_is_decimal(self):
        # Reporting both would duplicate the complaint for one bad value.
        assert non_negative("abc") is None

    def test_max_length(self):
        check = max_length(5)
        assert check("short") is None
        assert check("too long") is not None
        assert check(None) is None

    def test_one_of(self):
        check = one_of(["usd", "php"])
        assert check("usd") is None
        assert check("eur") is not None
        assert check(None) is None


class TestValidator:
    def validator(self, **kwargs) -> Validator:
        return Validator(
            {
                "id": [required],
                "amount": [required, is_decimal, non_negative],
                "currency": [one_of(["usd", "php"])],
            },
            **kwargs,
        )

    def test_all_valid_rows_pass_through(self):
        rows = [
            {"id": "1", "amount": "10.00", "currency": "usd"},
            {"id": "2", "amount": "20.00", "currency": "php"},
        ]
        result = self.validator().validate(rows)

        assert len(result.valid) == 2
        assert result.rejected == []
        assert result.rejection_rate == 0.0

    def test_a_bad_row_is_quarantined_without_failing_the_batch(self):
        rows = [{"id": "1", "amount": "10.00", "currency": "usd"}] * 19
        rows.append({"id": None, "amount": "5.00", "currency": "usd"})

        result = self.validator().validate(rows)

        # The good rows still load; the bad one is kept with its reason.
        assert len(result.valid) == 19
        assert len(result.rejected) == 1
        assert "id is required" in result.rejected[0].reasons

    def test_a_row_collects_every_failure_reason(self):
        rows = [{"id": None, "amount": "-5", "currency": "eur"}]
        validator = Validator(
            {
                "id": [required],
                "amount": [non_negative],
                "currency": [one_of(["usd"])],
            },
            error_threshold=1.0,
        )
        result = validator.validate(rows)

        reasons = result.rejected[0].reasons
        assert len(reasons) == 3

    def test_threshold_breach_raises(self):
        rows = [
            {"id": "1", "amount": "10", "currency": "usd"},
            {"id": None, "amount": "10", "currency": "usd"},
        ]
        with pytest.raises(ValidationError) as exc:
            self.validator(error_threshold=0.05).validate(rows)

        assert exc.value.rejected == 1
        assert exc.value.total == 2
        assert exc.value.rate == 0.5

    def test_rate_exactly_at_the_threshold_passes(self):
        rows = [{"id": "1", "amount": "10", "currency": "usd"}] * 9
        rows.append({"id": None, "amount": "10", "currency": "usd"})

        # 10% rejected against a 10% threshold: a breach is strictly above it.
        result = self.validator(error_threshold=0.10).validate(rows)
        assert len(result.rejected) == 1

    def test_an_empty_batch_does_not_raise(self):
        result = self.validator(error_threshold=0.0).validate([])
        assert result.total == 0
        assert result.rejection_rate == 0.0

    def test_zero_threshold_rejects_any_bad_row(self):
        rows = [{"id": "1", "amount": "10", "currency": "usd"}] * 99
        rows.append({"id": None, "amount": "10", "currency": "usd"})

        with pytest.raises(ValidationError):
            self.validator(error_threshold=0.0).validate(rows)

    def test_unknown_columns_are_ignored(self):
        rows = [{"id": "1", "amount": "10", "currency": "usd", "extra": "ignored"}]
        result = self.validator().validate(rows)
        assert len(result.valid) == 1

    def test_a_missing_optional_column_is_fine(self):
        rows = [{"id": "1", "amount": "10"}]
        result = self.validator().validate(rows)
        assert len(result.valid) == 1

    def test_invalid_threshold_is_rejected(self):
        with pytest.raises(ValueError):
            Validator({}, error_threshold=1.5)
        with pytest.raises(ValueError):
            Validator({}, error_threshold=-0.1)

    def test_error_message_reports_the_rate(self):
        rows = [{"id": None, "amount": "10", "currency": "usd"}]
        with pytest.raises(ValidationError, match="100.0%"):
            self.validator(error_threshold=0.5).validate(rows)

"""Row-level validation that quarantines bad records instead of failing the batch.

An upstream feed will eventually send a malformed row. Aborting the whole run on
the first one means a single bad record blocks every good one behind it, and
silently dropping it means nobody finds out. Neither is acceptable in a nightly
load.

Rows that fail validation are routed to a quarantine table with the reason
attached, the good rows load normally, and the run fails only if the error rate
crosses a threshold — which is the signal that something upstream genuinely broke
rather than one record being odd.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Sequence


class ValidationError(Exception):
    """Raised when the share of rejected rows exceeds the configured threshold."""

    def __init__(self, rejected: int, total: int, threshold: float) -> None:
        rate = rejected / total if total else 0.0
        super().__init__(
            f"{rejected} of {total} rows rejected ({rate:.1%}), "
            f"above the {threshold:.1%} threshold"
        )
        self.rejected = rejected
        self.total = total
        self.rate = rate


@dataclass
class Rejection:
    row: dict[str, Any]
    reasons: list[str]


@dataclass
class ValidationResult:
    valid: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.valid) + len(self.rejected)

    @property
    def rejection_rate(self) -> float:
        return len(self.rejected) / self.total if self.total else 0.0


# A check returns None when the value is acceptable, or a reason when it is not.
Check = Callable[[Any], str | None]


def required(value: Any) -> str | None:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return "is required"
    return None


def is_decimal(value: Any) -> str | None:
    if value is None:
        return None
    try:
        Decimal(str(value))
    except (InvalidOperation, ValueError):
        return f"is not a number: {value!r}"
    return None


def non_negative(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if Decimal(str(value)) < 0:
            return f"must not be negative: {value!r}"
    except (InvalidOperation, ValueError):
        # is_decimal reports the type problem; do not duplicate it here.
        return None
    return None


def max_length(limit: int) -> Check:
    def check(value: Any) -> str | None:
        if value is not None and len(str(value)) > limit:
            return f"exceeds {limit} characters"
        return None

    return check


def one_of(allowed: Sequence[Any]) -> Check:
    permitted = set(allowed)

    def check(value: Any) -> str | None:
        if value is not None and value not in permitted:
            return f"is not one of {sorted(map(str, permitted))}"
        return None

    return check


class Validator:
    """Applies per-column checks to each row and partitions the results."""

    def __init__(
        self,
        checks: dict[str, list[Check]],
        *,
        error_threshold: float = 0.05,
    ) -> None:
        if not 0.0 <= error_threshold <= 1.0:
            raise ValueError("error_threshold must be between 0 and 1")
        self._checks = checks
        self._threshold = error_threshold

    def validate_row(self, row: dict[str, Any]) -> list[str]:
        reasons: list[str] = []
        for column, checks in self._checks.items():
            value = row.get(column)
            for check in checks:
                reason = check(value)
                if reason:
                    reasons.append(f"{column} {reason}")
        return reasons

    def validate(self, rows: Iterable[dict[str, Any]]) -> ValidationResult:
        """Partition rows into valid and rejected.

        Raises ``ValidationError`` when the rejection rate exceeds the threshold,
        which distinguishes "one odd record" from "the upstream feed changed".
        """
        result = ValidationResult()

        for row in rows:
            reasons = self.validate_row(row)
            if reasons:
                result.rejected.append(Rejection(row=row, reasons=reasons))
            else:
                result.valid.append(row)

        if result.total and result.rejection_rate > self._threshold:
            raise ValidationError(len(result.rejected), result.total, self._threshold)

        return result

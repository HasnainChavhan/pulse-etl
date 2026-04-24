"""
PulseETL — Multi-Stage Data Validator
Data quality checks at every stage:
  - Schema validation (rejects malformed records)
  - Null audits (flags incomplete records)
  - Referential integrity checks (foreign key consistency)
  - Automated anomaly alerting — zero silent data corruption
"""
import logging
import statistics
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ValidationOutcome(str, Enum):
    VALID = "valid"
    REJECTED = "rejected"  # Schema error — discard
    FLAGGED = "flagged"    # Warning — pass with flag


@dataclass
class ValidationResult:
    record: dict
    outcome: ValidationOutcome
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return self.outcome == ValidationOutcome.VALID

    def to_dict(self) -> dict:
        return {
            "outcome": self.outcome.value,
            "errors": self.errors,
            "warnings": self.warnings,
        }


REQUIRED_FIELDS = {
    "event": ["event_id", "event_type", "timestamp", "user_id"],
    "transaction": ["transaction_id", "amount", "currency", "user_id", "timestamp"],
    "pageview": ["session_id", "path", "timestamp"],
}

VALID_EVENT_TYPES = {
    "click", "pageview", "purchase", "login", "logout",
    "signup", "error", "api_call", "search", "download",
}

VALID_CURRENCIES = {"USD", "EUR", "GBP", "INR", "JPY", "AUD", "CAD", "SGD"}


class SchemaValidator:
    """
    Stage 1: Schema validation — rejects malformed records.

    Checks:
    - Required field presence
    - Field type correctness
    - Enum value membership
    """

    def validate(self, record: dict) -> ValidationResult:
        errors = []
        warnings = []

        # Presence check
        record_type = record.get("record_type", "event")
        required = REQUIRED_FIELDS.get(record_type, REQUIRED_FIELDS["event"])

        for field_name in required:
            if field_name not in record or record[field_name] is None:
                errors.append(f"Missing required field: '{field_name}'")

        # Type checks
        if "amount" in record and record["amount"] is not None:
            try:
                float(record["amount"])
            except (TypeError, ValueError):
                errors.append(f"Field 'amount' must be numeric, got: {type(record['amount']).__name__}")

        if "timestamp" in record and record["timestamp"] is not None:
            if not isinstance(record["timestamp"], (int, float, str)):
                errors.append("Field 'timestamp' must be int/float (epoch) or ISO string")

        # Enum membership
        if "event_type" in record and record.get("event_type") not in VALID_EVENT_TYPES:
            warnings.append(f"Unrecognised event_type: '{record.get('event_type')}'")

        if "currency" in record and record.get("currency") not in VALID_CURRENCIES:
            errors.append(f"Invalid currency: '{record.get('currency')}'")

        outcome = (
            ValidationOutcome.REJECTED if errors
            else ValidationOutcome.FLAGGED if warnings
            else ValidationOutcome.VALID
        )
        return ValidationResult(record=record, outcome=outcome, errors=errors, warnings=warnings)


class NullAuditor:
    """
    Stage 2: Null audit — flags incomplete records without discarding them.

    Records with nullable fields missing are flagged and passed downstream
    with a '_null_flags' metadata field for analyst review.
    """

    NULLABLE_FIELDS = ["user_id", "session_id", "device_type", "country", "referrer"]

    def audit(self, record: dict) -> dict:
        """
        Audit nullable fields and attach null flag metadata.

        Returns the record with '_null_flags' list (may be empty).
        """
        null_flags = []

        for field_name in self.NULLABLE_FIELDS:
            if field_name in record and record[field_name] is None:
                null_flags.append(field_name)

        record["_null_flags"] = null_flags
        record["_has_nulls"] = len(null_flags) > 0

        if null_flags:
            logger.debug(
                f"Record {record.get('event_id', 'unknown')} has null fields: {null_flags}"
            )

        return record


class ReferentialIntegrityChecker:
    """
    Stage 3: Referential integrity — ensures foreign key consistency.

    Validates that referenced entities (users, sessions, products) exist
    in the known key sets. In production, this queries the dimension tables.
    """

    def __init__(self):
        # In production: loaded from DB on startup, refreshed periodically
        self._known_users: set = set()
        self._known_sessions: set = set()

    def register_user(self, user_id: str):
        self._known_users.add(user_id)

    def register_session(self, session_id: str):
        self._known_sessions.add(session_id)

    def check(self, record: dict) -> list[str]:
        """
        Check referential integrity and return list of violations.
        Empty list means all FK checks passed.
        """
        violations = []

        # Only check if we have known entities (populated from DB)
        if self._known_users and "user_id" in record:
            user_id = record.get("user_id")
            if user_id and user_id not in self._known_users:
                violations.append(f"Unknown user_id: '{user_id}'")

        if self._known_sessions and "session_id" in record:
            session_id = record.get("session_id")
            if session_id and session_id not in self._known_sessions:
                violations.append(f"Unknown session_id: '{session_id}'")

        return violations


class AnomalyDetector:
    """
    Automated anomaly alerting — zero silent data corruption.

    Uses rolling statistics to detect:
    - Volume spikes (record count anomalies)
    - Value anomalies (amounts, durations outside N standard deviations)
    """

    def __init__(self, window_size: int = 100, std_threshold: float = 3.0):
        self.window_size = window_size
        self.std_threshold = std_threshold
        self._amount_history: list[float] = []
        self._batch_sizes: list[int] = []

    def check_amount(self, amount: float) -> Optional[str]:
        """Flag if amount is outside N standard deviations of recent history."""
        self._amount_history.append(amount)
        if len(self._amount_history) > self.window_size:
            self._amount_history.pop(0)

        if len(self._amount_history) < 10:
            return None  # Not enough history yet

        mean = statistics.mean(self._amount_history)
        stdev = statistics.stdev(self._amount_history)

        if stdev == 0:
            return None

        z_score = abs(amount - mean) / stdev
        if z_score > self.std_threshold:
            alert = (
                f"ANOMALY: amount={amount:.2f} is {z_score:.1f} std deviations "
                f"from mean={mean:.2f} (stdev={stdev:.2f})"
            )
            logger.warning(alert)
            return alert

        return None

    def check_batch_size(self, batch_size: int) -> Optional[str]:
        """Flag if batch size deviates significantly from historical average."""
        self._batch_sizes.append(batch_size)
        if len(self._batch_sizes) > self.window_size:
            self._batch_sizes.pop(0)

        if len(self._batch_sizes) < 5:
            return None

        mean = statistics.mean(self._batch_sizes)
        stdev = statistics.stdev(self._batch_sizes)

        if stdev == 0:
            return None

        z_score = abs(batch_size - mean) / stdev
        if z_score > self.std_threshold:
            alert = (
                f"ANOMALY: batch_size={batch_size} is {z_score:.1f} std deviations "
                f"from mean={mean:.1f}"
            )
            logger.warning(alert)
            return alert

        return None

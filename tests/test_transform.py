"""
PulseETL — Test Suite: Transform Pipeline & Validator
"""
import pytest
from app.transform.normalizer import Deduplicator, Normalizer
from app.transform.validator import (
    AnomalyDetector,
    NullAuditor,
    SchemaValidator,
    ValidationOutcome,
)


class TestNormalizer:

    def test_renames_field_aliases(self):
        normalizer = Normalizer()
        record = {"userId": "u123", "eventType": "click", "ts": 1700000000}
        result = normalizer.normalize(record)
        assert "user_id" in result
        assert result["user_id"] == "u123"
        assert "event_type" in result
        assert "timestamp" in result

    def test_coerces_amount_to_float(self):
        normalizer = Normalizer()
        record = {"event_id": "e1", "amt": "42.50"}
        result = normalizer.normalize(record)
        assert result["amount"] == 42.50
        assert isinstance(result["amount"], float)

    def test_invalid_amount_becomes_none(self):
        normalizer = Normalizer()
        record = {"amt": "not-a-number"}
        result = normalizer.normalize(record)
        assert result["amount"] is None

    def test_parses_unix_epoch_timestamp(self):
        normalizer = Normalizer()
        record = {"ts": 1700000000}
        result = normalizer.normalize(record)
        assert "2023" in result["timestamp"] or "2024" in result["timestamp"]

    def test_parses_millisecond_epoch(self):
        normalizer = Normalizer()
        record = {"ts": 1700000000000}
        result = normalizer.normalize(record)
        assert result["timestamp"] is not None

    def test_uppercases_event_type_and_currency(self):
        normalizer = Normalizer()
        record = {"event_type": "purchase", "currency": "usd"}
        result = normalizer.normalize(record)
        assert result["event_type"] == "PURCHASE"
        assert result["currency"] == "USD"

    def test_normalize_batch(self):
        normalizer = Normalizer()
        batch = [{"userId": f"u{i}", "ts": 1700000000} for i in range(5)]
        results = normalizer.normalize_batch(batch)
        assert len(results) == 5
        assert all("user_id" in r for r in results)


class TestDeduplicator:

    def test_first_record_passes(self):
        dedup = Deduplicator(window_seconds=60)
        record = {"event_id": "e1", "user_id": "u1", "event_type": "click", "timestamp": "2024-01-01"}
        result = dedup.deduplicate(record)
        assert result is not None

    def test_duplicate_is_dropped(self):
        dedup = Deduplicator(window_seconds=60)
        record = {"event_id": "e1", "user_id": "u1", "event_type": "click", "timestamp": "2024-01-01"}
        dedup.deduplicate(record)
        result = dedup.deduplicate(record)
        assert result is None
        assert dedup.duplicates_dropped == 1

    def test_different_events_pass(self):
        dedup = Deduplicator(window_seconds=60)
        r1 = {"event_id": "e1", "user_id": "u1", "event_type": "click", "timestamp": "2024-01-01"}
        r2 = {"event_id": "e2", "user_id": "u1", "event_type": "purchase", "timestamp": "2024-01-01"}
        assert dedup.deduplicate(r1) is not None
        assert dedup.deduplicate(r2) is not None

    def test_batch_dedup(self):
        dedup = Deduplicator()
        record = {"event_id": "e1", "user_id": "u1", "event_type": "click", "timestamp": "t1"}
        batch = [record, record, record]
        results = dedup.deduplicate_batch(batch)
        assert len(results) == 1


class TestSchemaValidator:

    def test_valid_event_passes(self):
        validator = SchemaValidator()
        record = {
            "record_type": "event",
            "event_id": "e1",
            "event_type": "CLICK",
            "timestamp": "2024-01-01T00:00:00Z",
            "user_id": "u1",
        }
        result = validator.validate(record)
        assert result.outcome == ValidationOutcome.VALID
        assert result.is_valid

    def test_missing_required_field_rejects(self):
        validator = SchemaValidator()
        record = {"event_type": "click"}  # Missing event_id, timestamp, user_id
        result = validator.validate(record)
        assert result.outcome == ValidationOutcome.REJECTED
        assert len(result.errors) > 0

    def test_invalid_currency_rejects(self):
        validator = SchemaValidator()
        record = {
            "event_id": "e1",
            "event_type": "PURCHASE",
            "timestamp": "2024-01-01T00:00:00Z",
            "user_id": "u1",
            "currency": "FAKE",
        }
        result = validator.validate(record)
        assert result.outcome == ValidationOutcome.REJECTED

    def test_unknown_event_type_flags(self):
        validator = SchemaValidator()
        record = {
            "event_id": "e1",
            "event_type": "UNKNOWN_EVENT",
            "timestamp": "2024-01-01T00:00:00Z",
            "user_id": "u1",
        }
        result = validator.validate(record)
        assert result.outcome == ValidationOutcome.FLAGGED
        assert len(result.warnings) > 0


class TestAnomalyDetector:

    def test_no_anomaly_with_insufficient_history(self):
        detector = AnomalyDetector(window_size=100, std_threshold=3.0)
        for i in range(5):
            result = detector.check_amount(100.0)
        # Not enough history
        assert result is None

    def test_detects_anomalous_amount(self):
        detector = AnomalyDetector(window_size=50, std_threshold=2.0)
        # Build history around 100.0
        for _ in range(20):
            detector.check_amount(100.0)
        # Inject a massive outlier
        result = detector.check_amount(10000.0)
        assert result is not None
        assert "ANOMALY" in result

    def test_normal_amounts_pass(self):
        detector = AnomalyDetector(window_size=50, std_threshold=3.0)
        for _ in range(20):
            detector.check_amount(100.0)
        # Slightly higher — should be fine
        result = detector.check_amount(105.0)
        assert result is None

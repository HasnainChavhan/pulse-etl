"""
PulseETL — Normalizer, Deduplicator Transform Stages
Stage 1: Normalisation  — canonical field names, type coercion, timestamp parsing
Stage 2: Deduplication — fingerprint-based sliding window dedup
"""
import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from app.core.config import settings

logger = logging.getLogger(__name__)


class Normalizer:
    """
    Stage 1: Data normalization.

    Transforms raw event records into a canonical schema:
    - Renames aliased field names to canonical names
    - Coerces types (e.g. string amounts → float)
    - Parses timestamps to ISO 8601 UTC
    - Strips whitespace and normalises string case where appropriate
    """

    FIELD_ALIASES = {
        "userId": "user_id",
        "user": "user_id",
        "uid": "user_id",
        "eventType": "event_type",
        "event": "event_type",
        "ts": "timestamp",
        "time": "timestamp",
        "sessionId": "session_id",
        "sid": "session_id",
        "amt": "amount",
        "price": "amount",
        "curr": "currency",
        "ccy": "currency",
        "evt_id": "event_id",
        "id": "event_id",
    }

    def normalize(self, record: dict) -> dict:
        """
        Normalize a single raw record to canonical schema.

        Args:
            record: Raw record from Kafka, may have aliased field names

        Returns:
            Normalized record dict with canonical field names and coerced types
        """
        normalized = {}

        # Rename aliases → canonical names
        for key, value in record.items():
            canonical = self.FIELD_ALIASES.get(key, key)
            normalized[canonical] = value

        # Type coercions
        if "amount" in normalized and normalized["amount"] is not None:
            try:
                normalized["amount"] = float(normalized["amount"])
            except (TypeError, ValueError):
                normalized["amount"] = None

        if "timestamp" in normalized:
            normalized["timestamp"] = self._parse_timestamp(normalized["timestamp"])

        # String normalization
        for str_field in ["event_type", "currency", "country"]:
            if str_field in normalized and isinstance(normalized[str_field], str):
                normalized[str_field] = normalized[str_field].strip().upper()

        for id_field in ["user_id", "session_id", "event_id"]:
            if id_field in normalized and isinstance(normalized[id_field], str):
                normalized[id_field] = normalized[id_field].strip()

        normalized["_normalized_at"] = datetime.now(timezone.utc).isoformat()
        return normalized

    def _parse_timestamp(self, ts) -> Optional[str]:
        """Parse various timestamp formats to ISO 8601 UTC."""
        if ts is None:
            return None

        try:
            if isinstance(ts, (int, float)):
                # Unix epoch (seconds or milliseconds)
                if ts > 1e12:
                    ts = ts / 1000  # Milliseconds
                return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            elif isinstance(ts, str):
                # Try ISO parse
                ts = ts.replace("Z", "+00:00")
                return datetime.fromisoformat(ts).astimezone(timezone.utc).isoformat()
        except (ValueError, TypeError, OSError) as e:
            logger.debug(f"Timestamp parse failed for {ts!r}: {e}")

        return str(ts)  # Fallback: keep as-is

    def normalize_batch(self, records: list[dict]) -> list[dict]:
        """Normalize a batch of records."""
        normalized = []
        for record in records:
            try:
                normalized.append(self.normalize(record))
            except Exception as e:
                logger.error(f"Normalization failed for record: {e}")
        return normalized


class Deduplicator:
    """
    Stage 2: Sliding window deduplication.

    Uses SHA-256 fingerprinting of key fields to detect duplicate events
    within a configurable time window. Duplicates are dropped silently
    with a counter increment (no silent corruption).
    """

    def __init__(self, window_seconds: Optional[int] = None):
        self.window_seconds = window_seconds or settings.dedup_window_seconds
        self._seen: dict[str, float] = {}  # fingerprint → first seen timestamp
        self.duplicates_dropped = 0
        self.total_processed = 0

    def _fingerprint(self, record: dict) -> str:
        """Generate a record fingerprint from key fields."""
        key_fields = (
            record.get("event_id", ""),
            record.get("user_id", ""),
            record.get("event_type", ""),
            record.get("timestamp", ""),
        )
        content = "|".join(str(f) for f in key_fields)
        return hashlib.sha256(content.encode()).hexdigest()[:20]

    def _evict_expired(self):
        """Remove fingerprints outside the dedup window."""
        cutoff = time.monotonic() - self.window_seconds
        expired = [fp for fp, ts in self._seen.items() if ts < cutoff]
        for fp in expired:
            del self._seen[fp]

    def deduplicate(self, record: dict) -> Optional[dict]:
        """
        Check if a record is a duplicate.

        Returns the record if new, None if duplicate.
        """
        self._evict_expired()
        fp = self._fingerprint(record)
        self.total_processed += 1

        if fp in self._seen:
            self.duplicates_dropped += 1
            logger.debug(f"Duplicate dropped: {fp} (event_id={record.get('event_id')})")
            return None

        self._seen[fp] = time.monotonic()
        return record

    def deduplicate_batch(self, records: list[dict]) -> list[dict]:
        """Deduplicate a batch of records."""
        return [r for r in (self.deduplicate(rec) for rec in records) if r is not None]

    def stats(self) -> dict:
        return {
            "total_processed": self.total_processed,
            "duplicates_dropped": self.duplicates_dropped,
            "dedup_rate_pct": (
                round(self.duplicates_dropped / self.total_processed * 100, 2)
                if self.total_processed > 0 else 0.0
            ),
            "window_size": len(self._seen),
            "window_seconds": self.window_seconds,
        }

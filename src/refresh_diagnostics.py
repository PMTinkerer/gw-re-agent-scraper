"""Bounded public-source summary evidence for offline parsing/coverage replay.

Only public markdown and allowlisted context are retained, never request headers,
provider envelopes, scripts or credentials. Files are locally 0600; workflow
artifact access inherits repository visibility. Truncation is explicit, not replayable
proof of a complete response. No retention or export to the source database.
"""

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .active_refresh import RefreshIncomplete


class SummaryDiagnostics:
    max_record_bytes = 2_000_000
    max_total_bytes = 100_000_000
    max_records = 1000

    def __init__(self, path, *, redact):
        self.path = Path(path)
        self.redact = redact

    def check_capacity(self):
        self.path.mkdir(parents=True, exist_ok=True, mode=0o700)
        records = list(self.path.glob("*.json"))
        if (
            len(records) >= self.max_records
            or sum(p.stat().st_size for p in records) + self.max_record_bytes
            > self.max_total_bytes
        ):
            raise RefreshIncomplete("Diagnostic capacity exhausted before scrape")

    def save(self, url, data):
        self.check_capacity()
        text = data.get("markdown")
        text = (
            text.replace(self.redact, "[REDACTED]") if isinstance(text, str) else None
        )
        metadata = data.get("metadata")
        status = metadata.get("statusCode") if isinstance(metadata, dict) else None
        record = dict(
            schema_version=1,
            url=url,
            captured_at=datetime.now(timezone.utc).isoformat(),
            status_code=status if type(status) is int else None,
            markdown=text,
            content_sha256=(
                hashlib.sha256(text.encode()).hexdigest() if text is not None else None
            ),
            truncated=False,
        )

        # Binary search bounds JSON-encoded bytes, including multibyte/escaped text.
        def encode():
            return (json.dumps(record, ensure_ascii=False) + "\n").encode()

        payload = encode()
        if len(payload) > self.max_record_bytes:
            record["truncated"] = True
            low, high = 0, len(text or "")
            while low < high:
                mid = (low + high + 1) // 2
                record["markdown"] = text[:mid]
                if len(encode()) <= self.max_record_bytes:
                    low = mid
                else:
                    high = mid - 1
            record["markdown"] = text[:low]
            payload = encode()
        if len(payload) > self.max_record_bytes:
            raise RefreshIncomplete("Diagnostic metadata exceeds record limit")
        path = self.path / (uuid.uuid4().hex + ".json")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)

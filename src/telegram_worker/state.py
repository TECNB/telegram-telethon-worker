from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


class StateStore:
    """Small JSON cursors kept compatible with existing deployments."""

    def __init__(self, path: Path):
        self.path = path

    def cursor(self, source_id: int) -> Optional[int]:
        return self._read().get(str(source_id))

    def save_cursor(self, source_id: int, message_id: int) -> None:
        state = self._read()
        state[str(source_id)] = message_id
        self._write_json(self.path, state)

    def backfill_before(self, source_id: int) -> Optional[int]:
        return self._read_int_map(self.backfill_path).get(str(source_id))

    def save_backfill_before(self, source_id: int, message_id: int) -> None:
        state = self._read_int_map(self.backfill_path)
        state[str(source_id)] = message_id
        self._write_json(self.backfill_path, state)

    def reconciliation_cursor(self, target_id: int) -> Optional[int]:
        return self._read_int_map(self.reconcile_path).get(str(target_id))

    def save_reconciliation_cursor(self, target_id: int, message_id: int) -> None:
        state = self._read_int_map(self.reconcile_path)
        state[str(target_id)] = message_id
        self._write_json(self.reconcile_path, state)

    # Legacy progress remains readable during the transition to the delivery ledger.
    def completed_groups(self, source_id: int, anchor_id: int) -> set[str]:
        return set(self._read_progress().get(f"{source_id}:{anchor_id}", []))

    def save_completed_group(self, source_id: int, anchor_id: int, group_key: str) -> None:
        progress = self._read_progress()
        key = f"{source_id}:{anchor_id}"
        progress[key] = sorted(set(progress.get(key, [])) | {group_key})
        self._write_json(self.progress_path, progress)

    def clear_progress(self, source_id: int, anchor_id: int) -> None:
        progress = self._read_progress()
        if progress.pop(f"{source_id}:{anchor_id}", None) is not None:
            self._write_json(self.progress_path, progress)

    @property
    def progress_path(self) -> Path:
        return self.path.with_suffix(".progress.json")

    @property
    def backfill_path(self) -> Path:
        return self.path.with_suffix(".backfill.json")

    @property
    def reconcile_path(self) -> Path:
        return self.path.with_suffix(".reconcile.json")

    @property
    def database_path(self) -> Path:
        return self.path.with_suffix(".db")

    def _read(self) -> Dict[str, int]:
        return self._read_int_map(self.path)

    def _read_progress(self) -> Dict[str, list[str]]:
        if not self.progress_path.exists():
            return {}
        value = json.loads(self.progress_path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or any(
            not isinstance(key, str)
            or not isinstance(items, list)
            or any(not isinstance(item, str) for item in items)
            for key, items in value.items()
        ):
            raise RuntimeError(f"Invalid progress file: {self.progress_path}")
        return value

    @staticmethod
    def _read_int_map(path: Path) -> Dict[str, int]:
        if not path.exists():
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or any(
            not isinstance(key, str) or isinstance(item, bool) or not isinstance(item, int)
            for key, item in value.items()
        ):
            raise RuntimeError(f"Invalid state file: {path}")
        return value

    @staticmethod
    def _write_json(path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
        temporary.replace(path)


@dataclass(frozen=True)
class Delivery:
    target_id: int
    source_id: int
    group_key: str
    source_message_ids: list[int]
    target_message_ids: list[int]
    delivery_mode: str
    delivered_at: str


class DeliveryLedger:
    """Permanent cross-mode deduplication for successfully delivered source groups."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS deliveries (
                    target_id INTEGER NOT NULL,
                    source_id INTEGER NOT NULL,
                    group_key TEXT NOT NULL,
                    resource_key TEXT NOT NULL,
                    source_message_ids TEXT NOT NULL,
                    target_message_ids TEXT NOT NULL,
                    delivery_mode TEXT NOT NULL,
                    request_id TEXT,
                    delivered_at TEXT NOT NULL,
                    PRIMARY KEY (target_id, source_id, group_key)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS command_results (
                    request_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    PRIMARY KEY (request_id, operation)
                )
                """
            )

    def get(self, target_id: int, source_id: int, group_key: str) -> Optional[Delivery]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT source_message_ids, target_message_ids, delivery_mode, delivered_at
                FROM deliveries
                WHERE target_id = ? AND source_id = ? AND group_key = ?
                """,
                (target_id, source_id, group_key),
            ).fetchone()
        if row is None:
            return None
        return Delivery(
            target_id,
            source_id,
            group_key,
            json.loads(row[0]),
            json.loads(row[1]),
            row[2],
            row[3],
        )

    def record(
        self,
        *,
        target_id: int,
        source_id: int,
        group_key: str,
        resource_key: str,
        source_message_ids: list[int],
        target_message_ids: list[int],
        delivery_mode: str,
        request_id: Optional[str] = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO deliveries (
                    target_id, source_id, group_key, resource_key,
                    source_message_ids, target_message_ids,
                    delivery_mode, request_id, delivered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_id,
                    source_id,
                    group_key,
                    resource_key,
                    json.dumps(source_message_ids),
                    json.dumps(target_message_ids),
                    delivery_mode,
                    request_id,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def count(self) -> int:
        with self._connect() as connection:
            return connection.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]

    def command_result(self, request_id: str, operation: str) -> Optional[dict]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT result_json
                FROM command_results
                WHERE request_id = ? AND operation = ?
                """,
                (request_id, operation),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def record_command_result(
        self, request_id: str, operation: str, result: dict
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO command_results (
                    request_id, operation, result_json, completed_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    request_id,
                    operation,
                    json.dumps(result, ensure_ascii=False),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

"""Gestão de lineage (rastreabilidade)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List


@dataclass
class LineageRecord:
    dataset: str
    sources: List[str]
    transformation: str
    executed_by: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    notes: str | None = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "dataset": self.dataset,
            "sources": self.sources,
            "transformation": self.transformation,
            "executed_by": self.executed_by,
            "timestamp": self.timestamp.isoformat(),
            "notes": self.notes,
        }


class LineageTracker:
    """Registro de lineage para auditorias."""

    def __init__(self) -> None:
        self._records: List[LineageRecord] = []

    def add_record(self, record: LineageRecord) -> None:
        self._records.append(record)

    def list_records(self) -> List[LineageRecord]:
        return list(self._records)

    def export(self) -> List[Dict[str, object]]:
        return [record.to_dict() for record in self._records]

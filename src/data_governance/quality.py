"""Validador de qualidade de dados."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List

import pandas as pd


@dataclass
class QualityIssue:
    dataset: str
    rule_name: str
    severity: str
    message: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "dataset": self.dataset,
            "rule_name": self.rule_name,
            "severity": self.severity,
            "message": self.message,
        }


class QualityValidator:
    """Aplica regras automáticas de qualidade."""

    def __init__(self, quality_rules: Dict[str, List[Dict[str, Any]]]) -> None:
        self.quality_rules = quality_rules

    def validate(self, dataset: str, df: pd.DataFrame) -> List[QualityIssue]:
        issues: List[QualityIssue] = []
        for rule in self.quality_rules.get(dataset, []):
            rule_type = rule.get("type")
            column = rule.get("column")
            if rule_type == "recency":
                issues.extend(self._check_recency(dataset, df, column, rule))
            elif rule_type == "pattern":
                issues.extend(self._check_pattern(dataset, df, column, rule))
            elif rule_type == "numeric_min":
                issues.extend(self._check_numeric_min(dataset, df, column, rule))
        return issues

    def _check_recency(
        self, dataset: str, df: pd.DataFrame, column: str, rule: Dict[str, Any]
    ) -> List[QualityIssue]:
        max_months = rule.get("max_months", 12)
        limit = datetime.utcnow().date().replace(day=1)
        issues: List[QualityIssue] = []
        for idx, value in df[column].items():
            try:
                date_value = datetime.fromisoformat(str(value)).date()
            except ValueError:
                issues.append(
                    QualityIssue(
                        dataset,
                        rule_name=rule["name"],
                        severity="high",
                        message=f"Linha {idx}: data inválida '{value}'.",
                    )
                )
                continue
            months_diff = (limit.year - date_value.year) * 12 + (limit.month - date_value.month)
            if months_diff > max_months:
                issues.append(
                    QualityIssue(
                        dataset,
                        rule_name=rule["name"],
                        severity="medium",
                        message=f"Linha {idx}: consentimento desatualizado ({months_diff} meses).",
                    )
                )
        return issues

    def _check_pattern(
        self, dataset: str, df: pd.DataFrame, column: str, rule: Dict[str, Any]
    ) -> List[QualityIssue]:
        import re

        pattern = re.compile(rule.get("regex", ".*"))
        issues: List[QualityIssue] = []
        for idx, value in df[column].items():
            if not pattern.match(str(value)):
                issues.append(
                    QualityIssue(
                        dataset,
                        rule_name=rule["name"],
                        severity="high",
                        message=f"Linha {idx}: valor '{value}' fora do padrão.",
                    )
                )
        return issues

    def _check_numeric_min(
        self, dataset: str, df: pd.DataFrame, column: str, rule: Dict[str, Any]
    ) -> List[QualityIssue]:
        min_value = rule.get("min_value", 0)
        issues: List[QualityIssue] = []
        for idx, value in df[column].items():
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                issues.append(
                    QualityIssue(
                        dataset,
                        rule_name=rule["name"],
                        severity="high",
                        message=f"Linha {idx}: valor '{value}' não numérico.",
                    )
                )
                continue
            if numeric_value <= min_value:
                issues.append(
                    QualityIssue(
                        dataset,
                        rule_name=rule["name"],
                        severity="medium",
                        message=f"Linha {idx}: valor {numeric_value} abaixo do mínimo {min_value}.",
                    )
                )
        return issues

    @staticmethod
    def summarize(issues: Iterable[QualityIssue]) -> List[Dict[str, str]]:
        return [issue.to_dict() for issue in issues]

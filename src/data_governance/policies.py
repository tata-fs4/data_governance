"""Carregamento de políticas regulatórias."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


class PolicyRegistry:
    """Mantém políticas LGPD e ISO em memória."""

    def __init__(self, policies_path: Path) -> None:
        self.policies_path = policies_path
        self._policies: Dict[str, Any] = {}

    def load(self) -> None:
        with self.policies_path.open("r", encoding="utf-8") as fp:
            self._policies = yaml.safe_load(fp)

    @property
    def regulations(self) -> Dict[str, Any]:
        return self._policies.get("regulations", {})

    @property
    def access_policies(self) -> Dict[str, Any]:
        return self._policies.get("access_policies", [])

    @property
    def quality_rules(self) -> Dict[str, Any]:
        return self._policies.get("quality_rules", {})

    @property
    def lineage(self) -> Dict[str, Any]:
        return self._policies.get("lineage", [])

    def as_dict(self) -> Dict[str, Any]:
        return dict(self._policies)

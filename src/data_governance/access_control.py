"""Gestão de acessos alinhada a LGPD e ISO."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set


@dataclass
class AccessPolicy:
    name: str
    description: str
    roles: List[str]
    datasets: List[str]
    permissions: Set[str]


class AccessController:
    """Controle de acesso baseado em políticas."""

    def __init__(self) -> None:
        self._policies: Dict[str, AccessPolicy] = {}

    def add_policy(self, policy: AccessPolicy) -> None:
        if policy.name in self._policies:
            raise ValueError(f"Política '{policy.name}' já cadastrada.")
        self._policies[policy.name] = policy

    def check_access(self, role: str, dataset: str, permission: str) -> bool:
        """Verifica se um papel tem permissão sobre um dataset."""

        for policy in self._policies.values():
            if role in policy.roles and dataset in policy.datasets:
                if permission in policy.permissions:
                    return True
        return False

    def export(self) -> List[Dict[str, object]]:
        """Exporta as políticas para auditoria."""

        return [
            {
                "name": policy.name,
                "description": policy.description,
                "roles": policy.roles,
                "datasets": policy.datasets,
                "permissions": sorted(policy.permissions),
            }
            for policy in self._policies.values()
        ]

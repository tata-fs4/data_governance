"""Módulo responsável pelo catálogo de dados."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class DataAsset:
    """Representa um dataset catalogado."""

    name: str
    description: str
    owner: str
    sensitivity: str
    tags: List[str]
    source_path: Path
    schema: Dict[str, str]
    regulations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """Serializa o ativo de dados para dicionário."""

        return {
            "name": self.name,
            "description": self.description,
            "owner": self.owner,
            "sensitivity": self.sensitivity,
            "tags": self.tags,
            "source_path": str(self.source_path),
            "schema": self.schema,
            "regulations": self.regulations,
        }


class DataCatalog:
    """Catálogo de dados com rastreabilidade e compliance."""

    def __init__(self) -> None:
        self._assets: Dict[str, DataAsset] = {}

    def register_asset(self, asset: DataAsset) -> None:
        """Registra um novo ativo de dados no catálogo."""

        if asset.name in self._assets:
            raise ValueError(f"Ativo '{asset.name}' já está cadastrado.")
        self._assets[asset.name] = asset

    def get_asset(self, name: str) -> Optional[DataAsset]:
        """Retorna um ativo de dados pelo nome."""

        return self._assets.get(name)

    def list_assets(self) -> List[DataAsset]:
        """Lista todos os ativos do catálogo."""

        return list(self._assets.values())

    def export(self) -> List[Dict[str, object]]:
        """Exporta o catálogo completo para uma lista de dicionários."""

        return [asset.to_dict() for asset in self._assets.values()]

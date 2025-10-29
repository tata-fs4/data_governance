"""Pipeline governado com compliance."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .access_control import AccessController, AccessPolicy
from .catalog import DataAsset, DataCatalog
from .lineage import LineageRecord, LineageTracker
from .policies import PolicyRegistry
from .quality import QualityValidator


class ComplianceException(Exception):
    """Exceções relacionadas a violações de compliance."""


class DataGovernancePipeline:
    """Pipeline de dados auditável e alinhado a LGPD/ISO."""

    def __init__(self, base_path: Path) -> None:
        self.base_path = base_path
        self.config_path = base_path / "config" / "policies.yaml"
        self.raw_path = base_path / "data" / "raw"
        self.processed_path = base_path / "data" / "processed"
        self.logs_path = base_path / "logs"

        self.policy_registry = PolicyRegistry(self.config_path)
        self.catalog = DataCatalog()
        self.access_controller = AccessController()
        self.lineage_tracker = LineageTracker()
        self.quality_validator: QualityValidator | None = None

    def setup(self) -> None:
        """Carrega políticas e inicializa componentes."""

        self.policy_registry.load()
        self.quality_validator = QualityValidator(self.policy_registry.quality_rules)
        for policy_cfg in self.policy_registry.access_policies:
            policy = AccessPolicy(
                name=policy_cfg["name"],
                description=policy_cfg.get("description", ""),
                roles=policy_cfg.get("roles", []),
                datasets=policy_cfg.get("datasets", []),
                permissions=set(policy_cfg.get("permissions", [])),
            )
            self.access_controller.add_policy(policy)
        self._register_catalog_assets()

    def _register_catalog_assets(self) -> None:
        """Registra ativos no catálogo com metadados."""

        assets_metadata: List[Dict[str, object]] = [
            {
                "name": "customers",
                "description": "Cadastro de clientes com status de consentimento LGPD.",
                "owner": "privacy_officer",
                "sensitivity": "alta",
                "tags": ["pessoal", "lgpd", "master"],
                "source": self.raw_path / "customers.csv",
                "schema": {
                    "customer_id": "int",
                    "name": "string",
                    "email": "string",
                    "phone": "string",
                    "consent_status": "string",
                    "last_update": "date",
                },
                "regulations": ["lgpd"],
            },
            {
                "name": "transactions",
                "description": "Transações financeiras por cliente.",
                "owner": "finance_lead",
                "sensitivity": "média",
                "tags": ["financeiro", "iso27001"],
                "source": self.raw_path / "transactions.csv",
                "schema": {
                    "transaction_id": "int",
                    "customer_id": "int",
                    "amount": "float",
                    "date": "date",
                    "channel": "string",
                },
                "regulations": ["lgpd", "iso_27001"],
            },
        ]

        for metadata in assets_metadata:
            asset = DataAsset(
                name=metadata["name"],
                description=metadata["description"],
                owner=metadata["owner"],
                sensitivity=metadata["sensitivity"],
                tags=metadata["tags"],
                source_path=metadata["source"],
                schema=metadata["schema"],
                regulations=metadata["regulations"],
            )
            self.catalog.register_asset(asset)

    def _enforce_access(self, role: str, dataset: str, permission: str) -> None:
        if not self.access_controller.check_access(role, dataset, permission):
            raise ComplianceException(
                f"Role '{role}' não possui '{permission}' no dataset '{dataset}'."
            )

    def _load_dataset(self, asset_name: str) -> pd.DataFrame:
        asset = self.catalog.get_asset(asset_name)
        if not asset:
            raise ValueError(f"Ativo '{asset_name}' não encontrado no catálogo.")
        if not asset.source_path.exists():
            raise FileNotFoundError(f"Arquivo {asset.source_path} não encontrado.")
        df = pd.read_csv(asset.source_path)
        df.columns = [col.strip() for col in df.columns]
        return df

    def run(self) -> Dict[str, object]:
        """Executa o pipeline com validações e lineage."""

        if self.quality_validator is None:
            raise RuntimeError("Pipeline não inicializado. Execute setup().")

        audit_log: Dict[str, object] = {
            "started_at": datetime.utcnow().isoformat(),
            "regulations": self.policy_registry.regulations,
            "catalog": [asset.to_dict() for asset in self.catalog.list_assets()],
            "access_policies": self.access_controller.export(),
            "quality_issues": [],
            "lineage": [],
        }

        # Carrega dados sob controle de acesso
        self._enforce_access("data_governance", "customers", "read")
        customers_df = self._load_dataset("customers")
        self._enforce_access("finance_analyst", "transactions", "read")
        transactions_df = self._load_dataset("transactions")

        # Valida qualidade
        for dataset_name, df in {
            "customers": customers_df,
            "transactions": transactions_df,
        }.items():
            issues = self.quality_validator.validate(dataset_name, df)
            audit_log["quality_issues"].extend(
                {"dataset": dataset_name, **issue.to_dict()} for issue in issues
            )

        # Transformação compliance: somente clientes com consentimento válido
        consented_customers = customers_df[customers_df["consent_status"] == "granted"].copy()
        consented_customers.to_csv(
            self.processed_path / "customers_consenting.csv", index=False
        )

        self.lineage_tracker.add_record(
            LineageRecord(
                dataset="customers_consenting",
                sources=[str(self.raw_path / "customers.csv")],
                transformation="Filtro de consentimento LGPD",
                executed_by="data_governance_pipeline",
                notes="Inclui somente consentimentos ativos",
            )
        )

        # Join transações + clientes consentidos
        enriched_transactions = transactions_df.merge(
            consented_customers[["customer_id", "name", "email"]],
            on="customer_id",
            how="inner",
            validate="many_to_one",
        )
        enriched_transactions.to_csv(
            self.processed_path / "transactions_with_customers.csv", index=False
        )
        self.lineage_tracker.add_record(
            LineageRecord(
                dataset="transactions_with_customers",
                sources=[
                    str(self.raw_path / "transactions.csv"),
                    str(self.processed_path / "customers_consenting.csv"),
                ],
                transformation="Join com clientes com consentimento vigente",
                executed_by="data_governance_pipeline",
            )
        )

        audit_log["lineage"] = self.lineage_tracker.export()
        audit_log["finished_at"] = datetime.utcnow().isoformat()

        self._persist_audit_log(audit_log)
        return audit_log

    def _persist_audit_log(self, audit_log: Dict[str, object]) -> None:
        self.logs_path.mkdir(parents=True, exist_ok=True)
        audit_file = self.logs_path / f"audit_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
        with audit_file.open("w", encoding="utf-8") as fp:
            json.dump(audit_log, fp, ensure_ascii=False, indent=2)


def run_pipeline(base_path: str | Path) -> Dict[str, object]:
    pipeline = DataGovernancePipeline(Path(base_path))
    pipeline.setup()
    return pipeline.run()


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parents[2]
    audit = run_pipeline(base_dir)
    print(json.dumps(audit, indent=2, ensure_ascii=False))

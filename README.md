# Projeto de Governança de Dados

Este projeto demonstra um pipeline de governança de dados orientado a compliance, com catálogo, controle de acesso, lineage e validações automáticas de qualidade. O foco é alinhar a entrega de dados às diretrizes da LGPD e da ISO 27001.

## Estrutura

```
data_governance/
├── config/
│   └── policies.yaml        # Políticas regulatórias, regras de acesso e qualidade
├── data/
│   ├── raw/                 # Fontes originais mockadas
│   └── processed/           # Saídas governadas pelo pipeline
├── logs/                    # Registros de auditoria
├── src/
│   └── data_governance/     # Código fonte do framework
│       ├── access_control.py
│       ├── catalog.py
│       ├── lineage.py
│       ├── pipeline.py
│       ├── policies.py
│       └── quality.py
└── README.md
```

## Funcionalidades

- **Catálogo de Dados**: registra ativos com metadados, sensibilidade, tags e aderência regulatória.
- **Controle de Acesso**: aplica políticas baseadas em papéis alinhadas a LGPD e ISO 27001.
- **Lineage**: rastreia transformações e relaciona fontes e outputs para auditoria.
- **Qualidade Automática**: valida recência de consentimento, padrões de e-mail e valores monetários.
- **Auditoria**: gera logs JSON com evidências de execução, políticas carregadas e issues de qualidade.

## Executando o Pipeline

1. Crie um ambiente virtual opcionalmente e instale as dependências:
   ```bash
   pip install pandas pyyaml
   ```
2. Execute o pipeline:
   ```bash
   python -m data_governance.src.data_governance.pipeline
   ```

### Execução programática

```python
from data_governance.src.data_governance.pipeline import run_pipeline

audit_log = run_pipeline("data_governance")
print(audit_log)
```

## Conformidade

- **LGPD**: o pipeline filtra clientes sem consentimento válido e rastreia a última atualização.
- **ISO 27001**: políticas de acesso documentadas e controle de lineage fornecem evidências para auditorias.

## Próximos Passos

- Integrar com um catálogo corporativo (ex.: Apache Atlas).
- Automatizar ingestão de políticas de um GRC.
- Expandir regras de qualidade com perfis estatísticos.

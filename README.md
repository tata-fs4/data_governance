# Data Governance Project

This project demonstrates a data governance pipeline focused on compliance, featuring data cataloging, access control, lineage tracking, and automated quality validations.  
The goal is to align data delivery with **LGPD** and **ISO 27001** guidelines.

## Structure

```
data_governance/
├── config/
│ └── policies.yaml # Regulatory policies, access rules, quality rules
├── data/
│ ├── raw/ # Mocked source data
│ └── processed/ # Governed outputs produced by the pipeline
├── logs/ # Audit records
├── src/
│ └── data_governance/ # Framework implementation
│ ├── access_control.py
│ ├── catalog.py
│ ├── lineage.py
│ ├── pipeline.py
│ ├── policies.py
│ └── quality.py
└── README.md
```


## Features

- **Data Catalog**  
  Registers assets with metadata, sensitivity classification, tags, and regulatory compliance attributes.

- **Access Control**  
  Enforces role-based policies aligned with LGPD and ISO 27001 requirements.

- **Lineage Tracking**  
  Captures end-to-end transformations, mapping inputs and outputs for auditability.

- **Automated Data Quality**  
  Validates consent recency, email format, monetary values, and other business rules.

- **Auditing**  
  Generates JSON logs containing evidence of execution, loaded policies, and identified quality issues.

## Running the Pipeline

1. (Optional) Create a virtual environment and install dependencies:
   ```bash
   pip install pandas pyyaml

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

## Compliance

### LGPD
- The pipeline filters customers without valid consent and tracks the latest update timestamp.

### ISO 27001
- Documented access policies and lineage records provide evidence for security and compliance audits.

## Next Steps

- Integrate with a corporate catalog (e.g., Apache Atlas).
- Automate ingestion of policies from a GRC platform.
- Extend quality rules with statistical profiling and anomaly detection.

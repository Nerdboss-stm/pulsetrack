# HIPAA Compliance Strategy — PulseTrack

## De-Identification Approach

PulseTrack uses **HIPAA Safe Harbor** de-identification (45 CFR §164.514(b)).
All 18 PHI identifiers are removed or transformed before data reaches the Gold layer.

### PHI Handling by Layer

| Layer | PHI Present? | Handling |
|-------|-------------|---------|
| Bronze | Yes — raw events | Encrypted at rest, restricted access |
| Silver | Partial — masked | Email → SHA-256 hash, MRN → bridge key |
| Gold | No | All PHI removed or generalized |

### Specific Transformations

| PHI Element | Bronze | Silver | Gold |
|-------------|--------|--------|------|
| Patient name | Raw | Not stored | Not stored |
| Patient email | Raw | Used to derive patient_key | Not stored |
| Date of birth | Raw | Used to compute age | `age_group` (decade buckets) |
| Address | Raw | Not stored | Not stored |
| Phone number | Raw | Not stored | Not stored |
| MRN (hospital) | Raw | Bridge → SHA-256 patient_key | Hashed long key only |
| Device account ID | Raw | Stored for linking | Not stored |
| Geographic data | Raw (city/state) | Stored | Not stored |
| Date of service | Raw timestamps | Full timestamps | `date_key` (YYYYMMDD integer) |

### patient_key Derivation

```python
# Silver: SHA-256 hash of lowercase trimmed email (STRING)
patient_key_sha256 = sha2(lower(trim(patient_email)), 256)

# Gold: converted to LongType for JOIN efficiency
patient_key_long = abs(hash(patient_key_sha256)).cast("long")
```

The SHA-256 of email is a **one-way transformation** — the original email cannot be recovered from the key. This satisfies expert determination de-identification.

### age_group Buckets

Gold stores decade-level age buckets instead of exact ages:
- `<20`, `20-29`, `30-39`, `40-49`, `50-59`, `60-69`, `70+`

This prevents re-identification via age when combined with other quasi-identifiers.

## Access Control

### Role-Based Access

| Role | Bronze | Silver | Gold (fact tables) | Gold (dim_patient) |
|------|--------|--------|-------------------|-------------------|
| `data_engineer` | Read/Write | Read/Write | Read/Write | Read/Write |
| `clinical_analyst` | No | No | Read | Read (masked) |
| `researcher` | No | No | Read (aggregated only) | No |
| `airflow_service` | Read/Write | Read/Write | Write | Write |

Researchers are restricted to `vw_population_analytics` (no patient_key exposed).

## Data Retention

### HIPAA Requirement
HIPAA requires 6 years of PHI record retention (not the same as 7-year rule, which is for medical records).

### Resolution
PulseTrack uses **de-identification + retention**:
1. On patient deletion request: remove PII from Bronze/Silver, keep de-identified Gold records
2. Maintain audit log of deletion actions
3. Gold records are retained indefinitely (they contain no PHI by construction)

### Deletion Workflow
```bash
# Step 1: Remove from Bronze
python monitoring/hipaa_delete.py --patient_key <sha256_key> --layer bronze

# Step 2: Remove from Silver (identity bridge)
python monitoring/hipaa_delete.py --patient_key <sha256_key> --layer silver

# Step 3: Audit log entry
python monitoring/hipaa_delete.py --patient_key <sha256_key> --audit-only
```

## Audit Controls

### Weekly Audit DAG
`dag_hipaa_audit` (scheduled weekly, Sunday 3AM):
- Scans Gold tables for PII column names matching known PHI identifiers
- Checks dim_patient for any raw email/name patterns in text columns
- Verifies age_group values are within expected decade buckets
- Reports coverage of patient_key masking

### Audit Trail
All pipeline runs log:
- Which tables were written and row counts
- Identity resolution coverage %
- Any PII scan failures (immediately paged to data_engineer)

## Encryption

| Storage | Encryption |
|---------|-----------|
| Local Delta (/tmp/) | Filesystem encryption (FileVault/LUKS) |
| Azure Blob | AES-256 at rest (Azure-managed keys) |
| In-transit | TLS 1.2+ (Kafka with SASL/SSL, Spark → Azure) |
| PostgreSQL | pgcrypto extension for sensitive columns |

## Interview Talking Points

1. **Why not store exact age?** Re-identification risk: age + gender + rare condition → 87% of US population is uniquely identifiable (Sweeney 2000). Decade buckets reduce this dramatically.

2. **Why SHA-256 and not a random UUID?** SHA-256 of email is deterministic — the same patient arriving via EHR and via device gets the same patient_key without needing a lookup table. Random UUID would require a persistent mapping store.

3. **Why delete PII from Bronze but keep Gold?** HIPAA's purpose is to protect identifiable health information. Gold has no identifiers — it's already de-identified. Keeping Gold enables longitudinal population analytics without PHI risk.

4. **What's the difference between HIPAA de-identification and anonymization?** De-identification (Safe Harbor) removes specific identifiers. True anonymization (k-anonymity, differential privacy) provides mathematical guarantees. PulseTrack uses Safe Harbor for practical compliance; a research platform would add differential privacy.

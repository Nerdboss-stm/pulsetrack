"""Debug join keys + anomaly thresholds."""
import sys
from pathlib import Path
sys.path.insert(0, "/Users/nerdboss-stm/pulsetrack-cm")
import snowflake.connector
from pt_secrets import get_secret

creds = get_secret("snowflake")
conn = snowflake.connector.connect(
    account=creds["account"], user=creds["user"], password=creds["password"],
    role=creds["role"], warehouse=creds["warehouse"], database=creds["database"],
    schema="ANALYTICS",
)
cur = conn.cursor()

print("=" * 78)
print("DEBUG 1: identity_bridge samples (patient_key, identifier_type, identifier_value)")
print("=" * 78)
cur.execute("""
    SELECT identifier_type, link_status, COUNT(*) AS n,
           ANY_VALUE(patient_key) AS sample_patient_key,
           ANY_VALUE(identifier_value) AS sample_identifier_value
    FROM PULSETRACK.SILVER.IDENTITY_BRIDGE
    GROUP BY 1, 2 ORDER BY 1
""")
for r in cur.fetchall():
    print(f"  type={r[0]:<24} status={r[1]:<22} n={r[2]:<5} key={str(r[3])[:30]:<32} value={str(r[4])[:30]}")

print()
print("=" * 78)
print("DEBUG 2: ehr_conditions samples (patient_id, patient_email)")
print("=" * 78)
cur.execute("""
    SELECT patient_id, patient_email, COUNT(*) AS n
    FROM PULSETRACK.SILVER.EHR_CONDITIONS
    GROUP BY 1, 2 LIMIT 5
""")
for r in cur.fetchall():
    print(f"  patient_id={str(r[0])[:30]:<32} email={str(r[1])[:35]:<37} n={r[2]}")

print()
print("=" * 78)
print("DEBUG 3: dim_patient.patient_key sample (vs identity_bridge.patient_key type)")
print("=" * 78)
cur.execute("SELECT patient_key, age_group, gender FROM PULSETRACK.GOLD.DIM_PATIENT LIMIT 3")
for r in cur.fetchall():
    print(f"  patient_key={str(r[0])[:30]:<32} age={r[1]}  gender={r[2]}")

print()
print("=" * 78)
print("DEBUG 4: dim_patient.patient_id_masked sample")
print("=" * 78)
cur.execute("SELECT patient_id_masked, patient_key, age_group FROM PULSETRACK.GOLD.DIM_PATIENT LIMIT 5")
for r in cur.fetchall():
    print(f"  masked={str(r[0])[:40]:<42} key={r[1]:<12} age={r[2]}")

print()
print("=" * 78)
print("DEBUG 4b: Try SHA1(patient_id) = patient_id_masked")
print("=" * 78)
cur.execute("""
    SELECT
        (SELECT COUNT(DISTINCT p.patient_key)
         FROM PULSETRACK.GOLD.DIM_PATIENT p
         INNER JOIN PULSETRACK.SILVER.EHR_CONDITIONS c
            ON SHA1(c.patient_id) = p.patient_id_masked
        ) AS sha1_match,
        (SELECT COUNT(DISTINCT p.patient_key)
         FROM PULSETRACK.GOLD.DIM_PATIENT p
         INNER JOIN PULSETRACK.SILVER.EHR_CONDITIONS c
            ON SHA1(c.patient_email) = p.patient_id_masked
        ) AS sha1_email_match,
        (SELECT COUNT(DISTINCT p.patient_key)
         FROM PULSETRACK.GOLD.DIM_PATIENT p
         INNER JOIN PULSETRACK.SILVER.EHR_CONDITIONS c
            ON SHA2(c.patient_id, 256) = p.patient_id_masked
        ) AS sha256_match,
        (SELECT COUNT(DISTINCT p.patient_key)
         FROM PULSETRACK.GOLD.DIM_PATIENT p
         INNER JOIN PULSETRACK.SILVER.IDENTITY_BRIDGE b
            ON b.patient_key = p.patient_id_masked
        ) AS direct_match,
        (SELECT MAX(LENGTH(patient_id_masked)) FROM PULSETRACK.GOLD.DIM_PATIENT) AS masked_len,
        (SELECT MAX(LENGTH(patient_key)) FROM PULSETRACK.SILVER.IDENTITY_BRIDGE WHERE patient_key IS NOT NULL) AS bridge_len
""")
r = cur.fetchone()
print(f"  SHA1(patient_id) match: {r[0]}")
print(f"  SHA1(patient_email) match: {r[1]}")
print(f"  SHA2(patient_id, 256) match: {r[2]}")
print(f"  Direct bridge.patient_key=patient_id_masked match: {r[3]}")
print(f"  dim_patient.patient_id_masked length: {r[4]}")
print(f"  identity_bridge.patient_key length:   {r[5]}")

print()
print("=" * 78)
print("DEBUG 5: anomaly dashboard breakdown by status")
print("=" * 78)
cur.execute("""
    SELECT vital_status, severity_label, COUNT(*) AS n
    FROM PULSETRACK.ANALYTICS.VW_ANOMALY_DASHBOARD
    GROUP BY 1, 2 ORDER BY 3 DESC
""")
for r in cur.fetchall():
    print(f"  status={r[0]:<12} severity={r[1]:<24} n={r[2]:>6}")

print()
print("=" * 78)
print("DEBUG 5b: fact_vital_reading.is_valid distribution")
print("=" * 78)
cur.execute("""
    SELECT is_valid, is_late_arriving, COUNT(*) AS n
    FROM PULSETRACK.GOLD.FACT_VITAL_READING
    GROUP BY 1, 2 ORDER BY 1, 2
""")
for r in cur.fetchall():
    print(f"  is_valid={r[0]}  is_late_arriving={r[1]}  n={r[2]:>8}")

print()
print("=" * 78)
print("DEBUG 5c: Sample warning rows from vw_anomaly_dashboard")
print("=" * 78)
cur.execute("""
    SELECT metric_name, metric_value, vital_status, severity_label,
           TO_VARCHAR(event_timestamp) AS ts
    FROM PULSETRACK.ANALYTICS.VW_ANOMALY_DASHBOARD LIMIT 5
""")
for r in cur.fetchall():
    print(f"  metric={r[0]:<24} val={r[1]:<8.2f} status={r[2]:<10} sev={r[3]:<24} ts={r[4]}")

print()
print("=" * 78)
print("DEBUG 6: dim_metric ranges + actual fact_vital_reading.value distribution per metric")
print("=" * 78)
cur.execute("""
    WITH ranges AS (
      SELECT metric_name, normal_low, normal_high
      FROM PULSETRACK.GOLD.DIM_METRIC
    ),
    facts AS (
      SELECT f.metric_key, MIN(f.value) AS mn, MAX(f.value) AS mx, AVG(f.value) AS av, COUNT(*) AS n
      FROM PULSETRACK.GOLD.FACT_VITAL_READING f
      GROUP BY 1
    )
    SELECT r.metric_name, r.normal_low, r.normal_high, f.mn, f.mx, ROUND(f.av,2) AS avg_val, f.n
    FROM facts f
    INNER JOIN PULSETRACK.GOLD.DIM_METRIC m USING (metric_key)
    INNER JOIN ranges r USING (metric_name)
    ORDER BY n DESC LIMIT 8
""")
for r in cur.fetchall():
    print(f"  metric={r[0]:<24} normal=[{r[1]},{r[2]}]  actual=[{r[3]:.1f}, {r[4]:.1f}] avg={r[5]} n={r[6]}")

cur.close()
conn.close()

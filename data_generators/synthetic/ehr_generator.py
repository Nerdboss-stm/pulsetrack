"""
PulseTrack EHR FHIR Bundle Generator
======================================
Generates daily batch files simulating hospital EHR exports in FHIR format.

OUTPUT: JSON files in data/ehr_batches/YYYY-MM-DD/batch.json
Each file contains patient records with: conditions, medications, lab results.

THIS IS A BATCH SOURCE (not streaming):
- Files appear once per day
- Airflow detects new files and triggers ingestion
- Different ingestion pattern from Kafka streaming

FHIR (Fast Healthcare Interoperability Resources) is the healthcare data standard.
Real hospitals export data in this format.

PDF: System Design pages 57-63 (orchestration for batch sources)
"""

import json, os, random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

OUTPUT_DIR = "data/ehr_batches"

ICD10_CONDITIONS = [
    {"code": "E11.9", "desc": "Type 2 diabetes without complications", "category": "Endocrine", "chronic": True},
    {"code": "I10", "desc": "Essential hypertension", "category": "Circulatory", "chronic": True},
    {"code": "J45.20", "desc": "Mild intermittent asthma, uncomplicated", "category": "Respiratory", "chronic": True},
    {"code": "E78.5", "desc": "Hyperlipidemia, unspecified", "category": "Endocrine", "chronic": True},
    {"code": "F32.1", "desc": "Major depressive disorder, single episode, moderate", "category": "Mental", "chronic": False},
    {"code": "M54.5", "desc": "Low back pain", "category": "Musculoskeletal", "chronic": False},
    {"code": "K21.0", "desc": "GERD with esophagitis", "category": "Digestive", "chronic": True},
]

MEDICATIONS = [
    {"name": "metformin", "generic": "Metformin HCl", "class": "Biguanides", "dosages": ["500mg", "850mg", "1000mg"]},
    {"name": "lisinopril", "generic": "Lisinopril", "class": "ACE Inhibitors", "dosages": ["5mg", "10mg", "20mg"]},
    {"name": "albuterol", "generic": "Albuterol Sulfate", "class": "Bronchodilators", "dosages": ["90mcg"]},
    {"name": "atorvastatin", "generic": "Atorvastatin Calcium", "class": "Statins", "dosages": ["10mg", "20mg", "40mg"]},
    {"name": "sertraline", "generic": "Sertraline HCl", "class": "SSRIs", "dosages": ["25mg", "50mg", "100mg"]},
    {"name": "omeprazole", "generic": "Omeprazole", "class": "PPIs", "dosages": ["20mg", "40mg"]},
]

LAB_TESTS = [
    {"code": "HbA1c", "unit": "%", "normal_low": 4.0, "normal_high": 5.6},
    {"code": "LDL", "unit": "mg/dL", "normal_low": 0, "normal_high": 100},
    {"code": "BP_systolic", "unit": "mmHg", "normal_low": 90, "normal_high": 120},
    {"code": "TSH", "unit": "mIU/L", "normal_low": 0.4, "normal_high": 4.0},
    {"code": "Creatinine", "unit": "mg/dL", "normal_low": 0.7, "normal_high": 1.3},
]


def generate_patient_bundle(patient_mrn):
    """Generate a FHIR-like bundle for one patient."""
    entries = []
    
    # 1-3 conditions per patient
    num_conditions = random.randint(1, 3)
    patient_conditions = random.sample(ICD10_CONDITIONS, num_conditions)
    
    for cond in patient_conditions:
        onset_days_ago = random.randint(30, 2000)
        status = random.choices(
            ["active", "remission", "resolved"],
            weights=[0.7, 0.15, 0.15]
        )[0]
        
        entries.append({
            "resource_type": "Condition",
            "code": cond["code"],
            "description": cond["desc"],
            "category": cond["category"],
            "is_chronic": cond["chronic"],
            "onset_date": (datetime.utcnow() - timedelta(days=onset_days_ago)).strftime("%Y-%m-%d"),
            "status": status,
            "clinician_npi": f"NPI-{random.randint(1000000000, 9999999999)}",
        })
    
    # 0-3 medications
    num_meds = random.randint(0, 3)
    patient_meds = random.sample(MEDICATIONS, min(num_meds, len(MEDICATIONS)))
    
    for med in patient_meds:
        start_days_ago = random.randint(10, 1000)
        is_active = random.random() < 0.8
        
        entries.append({
            "resource_type": "MedicationStatement",
            "medication": med["name"],
            "generic_name": med["generic"],
            "drug_class": med["class"],
            "dosage": random.choice(med["dosages"]),
            "frequency": random.choice(["once_daily", "twice_daily", "as_needed"]),
            "start_date": (datetime.utcnow() - timedelta(days=start_days_ago)).strftime("%Y-%m-%d"),
            "end_date": None if is_active else (datetime.utcnow() - timedelta(days=random.randint(1, start_days_ago))).strftime("%Y-%m-%d"),
            "status": "active" if is_active else "stopped",
            "prescriber_npi": f"NPI-{random.randint(1000000000, 9999999999)}",
        })
    
    # 0-2 lab results
    num_labs = random.randint(0, 2)
    patient_labs = random.sample(LAB_TESTS, min(num_labs, len(LAB_TESTS)))
    
    for lab in patient_labs:
        value = round(random.uniform(lab["normal_low"] * 0.7, lab["normal_high"] * 1.5), 1)
        entries.append({
            "resource_type": "Observation",
            "code": lab["code"],
            "value": value,
            "unit": lab["unit"],
            "reference_low": lab["normal_low"],
            "reference_high": lab["normal_high"],
            "is_abnormal": value < lab["normal_low"] or value > lab["normal_high"],
            "date": (datetime.utcnow() - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d"),
        })
    
    return {
        "resource_type": "Bundle",
        "patient_id": patient_mrn,
        "patient_name": fake.name(),
        "patient_email": fake.email(),
        "patient_birth_year": random.randint(1945, 2002),
        "patient_zip": fake.zipcode(),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "entries": entries,
    }


def generate_daily_batch(date_str=None, num_patients=50):
    """Generate one day's EHR batch file."""
    if date_str is None:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
    
    batch_dir = os.path.join(OUTPUT_DIR, date_str)
    os.makedirs(batch_dir, exist_ok=True)
    
    patients = []
    for i in range(num_patients):
        mrn = f"MRN-{random.randint(10000, 99999)}-HOSP-A"
        bundle = generate_patient_bundle(mrn)
        patients.append(bundle)
    
    output_path = os.path.join(batch_dir, "ehr_batch.json")
    with open(output_path, 'w') as f:
        json.dump({"batch_date": date_str, "patient_count": len(patients), "patients": patients}, f, indent=2)
    
    print(f"  📋 Generated {len(patients)} patient records → {output_path}")
    return output_path


def main():
    print("🏥 EHR Batch Generator")
    print("   Generating last 7 days of EHR data...")
    
    for days_ago in range(7, -1, -1):
        date = (datetime.utcnow() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        generate_daily_batch(date, num_patients=random.randint(30, 70))
    
    print("\n✅ Done! Batch files in data/ehr_batches/")
    print("   In production, Airflow would detect these files and trigger ingestion.")

if __name__ == "__main__":
    main()
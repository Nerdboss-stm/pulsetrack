"""
PulseTrack Pharmacy CDC Generator
...
"""
import copy
import json
import os
import random
import sys
import time
from datetime import datetime

from kafka import KafkaProducer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402

log = get_logger(__name__)

# Fake database of active prescription fills (starts empty, gets populated as fills are created)
FILLS_DB = {}
NEXT_RX_NUM = 1000

# Drug reference data — ndc_code is the standard drug identifier
DRUGS = [
    {"ndc_code": "00093-7236-01", "drug_name": "Metformin HCl 500mg"},
    {"ndc_code": "00093-7237-01", "drug_name": "Metformin HCl 850mg"},
    {"ndc_code": "00093-7238-01", "drug_name": "Metformin HCl 1000mg"},
    {"ndc_code": "00093-5200-01", "drug_name": "Lisinopril 5mg"},
    {"ndc_code": "00093-5201-01", "drug_name": "Lisinopril 10mg"},
    {"ndc_code": "00093-5202-01", "drug_name": "Lisinopril 20mg"},
    {"ndc_code": "00113-0400-26", "drug_name": "Albuterol HFA 90mcg"},
    {"ndc_code": "00093-8167-01", "drug_name": "Atorvastatin 10mg"},
    {"ndc_code": "00093-8168-01", "drug_name": "Atorvastatin 20mg"},
    {"ndc_code": "00093-8169-01", "drug_name": "Atorvastatin 40mg"},
    {"ndc_code": "68180-0316-06", "drug_name": "Sertraline HCl 25mg"},
    {"ndc_code": "68180-0317-06", "drug_name": "Sertraline HCl 50mg"},
    {"ndc_code": "68180-0318-06", "drug_name": "Sertraline HCl 100mg"},
    {"ndc_code": "68180-0519-06", "drug_name": "Omeprazole 20mg"},
    {"ndc_code": "68180-0520-06", "drug_name": "Omeprazole 40mg"},
]

# 50 fake patient IDs
PATIENTS = [f"P{random.randint(100000, 999999)}" for _ in range(50)]


def generate_new_fill():
    global NEXT_RX_NUM
    drug = random.choice(DRUGS)
    patient_id = random.choice(PATIENTS)

    rx_id = f"RX-{NEXT_RX_NUM}"
    NEXT_RX_NUM += 1

    fill = {
        "rx_id": rx_id,
        "pharmacy_patient_id": patient_id,
        "ndc_code": drug["ndc_code"],
        "drug_name": drug["drug_name"],
        "fill_date": datetime.utcnow().strftime("%Y-%m-%d"),
        "days_supply": random.choice([30, 60, 90]),
        "refill_number": random.choices([0, 1, 2, 3], weights=[50, 25, 15, 10])[0],
        "status": "filled",
    }

    FILLS_DB[rx_id] = copy.deepcopy(fill)   # save to our fake DB

    return {
        "op": "c",
        "before": None,
        "after": copy.deepcopy(fill),
        "source": {"table": "prescription_fills", "db": "pharmacy_ops",
                   "ts_ms": int(datetime.utcnow().timestamp() * 1000)},
        "ts_ms": int(datetime.utcnow().timestamp() * 1000)
    }


def generate_status_change():
    if not FILLS_DB:
        return generate_new_fill()   # nothing to update yet, create instead

    rx_id = random.choice(list(FILLS_DB.keys()))
    fill = FILLS_DB[rx_id]

    before = copy.deepcopy(fill)
    new_status = random.choices(["returned", "cancelled", "transferred"], weights=[50, 30, 20])[0]

    fill["status"] = new_status

    after = copy.deepcopy(fill)

    del FILLS_DB[rx_id]   # returned = done, remove from active fills

    return {
        "op": "u",
        "before": before,
        "after": after,
        "source": {"table": "prescription_fills", "db": "pharmacy_ops",
                   "ts_ms": int(datetime.utcnow().timestamp() * 1000)},
        "ts_ms": int(datetime.utcnow().timestamp() * 1000)
    }


def main():
    log.info(
        "Pharmacy CDC generator starting",
        extra={"extra_data": {
            "topic": settings.kafka_topic_pharmacy,
            "changes_per_minute": settings.pharmacy_changes_per_minute,
        }},
    )

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    count = 0
    try:
        while True:
            # 70% new fill, 30% status change
            if random.random() < 0.70:
                event = generate_new_fill()
                change_type = "NEW FILL"
            else:
                event = generate_status_change()
                change_type = "STATUS CHANGE"

            producer.send(settings.kafka_topic_pharmacy, value=event)
            count += 1

            drug_name = (event.get("after") or event.get("before", {})).get("drug_name", "?")
            log.info(
                "Pharmacy CDC event",
                extra={"extra_data": {"seq": count, "type": change_type, "drug": drug_name}},
            )

            time.sleep(60.0 / settings.pharmacy_changes_per_minute)

    except KeyboardInterrupt:
        log.info("Pharmacy generator stopped", extra={"extra_data": {"total_events": count}})
        producer.close()


if __name__ == "__main__":
    main()

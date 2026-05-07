import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402

log = get_logger(__name__)

batch_path = os.path.join(settings.ehr_batch_dir, "2026-03-07", "ehr_batch.json")
with open(batch_path) as f:
    data = json.load(f)

top_keys = list(data.keys())
log.info("Top level keys", extra={"extra_data": {"keys": top_keys}})

patient_count = data["patient_count"]
log.info("Patient count", extra={"extra_data": {"patient_count": patient_count}})

p = data["patients"][0]
p_keys = list(p.keys())
log.info(
    "Patient bundle",
    extra={
        "extra_data": {
            "keys": p_keys,
            "patient_id": p["patient_id"],
            "patient_email": p["patient_email"],
        }
    },
)

entries = p["entries"]
log.info("Entries count", extra={"extra_data": {"count": len(entries)}})
for entry in entries:
    rtype = entry["resource_type"]
    keys = list(entry.keys())
    log.info(
        "Entry sample",
        extra={
            "extra_data": {
                "resource_type": rtype,
                "keys": keys,
                "sample": {k: entry[k] for k in list(entry.keys())[:4]},
            }
        },
    )

# Find a patient that has an Observation entry
for p in data["patients"]:
    for entry in p["entries"]:
        if entry["resource_type"] == "Observation":
            log.info(
                "Observation entry found",
                extra={
                    "extra_data": {
                        "patient_id": p["patient_id"],
                        "observation_keys": list(entry.keys()),
                        "full_entry": entry,
                    }
                },
            )
            break
    else:
        continue
    break

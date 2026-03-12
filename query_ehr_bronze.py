import json
with open('data/ehr_batches/2026-03-07/ehr_batch.json') as f:
    data = json.load(f)

top_keys = list(data.keys())
print('=== Top level keys ===')
print(top_keys)

patient_count = data['patient_count']
print('Patient count:', patient_count)

p = data['patients'][0]
p_keys = list(p.keys())
print('\n=== Patient bundle keys ===')
print(p_keys)
print('patient_id:', p['patient_id'])
print('patient_email:', p['patient_email'])

entries = p['entries']
print('\n=== Entries:', len(entries), '===')
for entry in entries:
    rtype = entry['resource_type']
    keys = list(entry.keys())
    print('  resource_type:', rtype)
    print('  keys:', keys)
    print('  sample:', {k: entry[k] for k in list(entry.keys())[:4]})
    print()

# Find a patient that has an Observation entry
for p in data['patients']:
    for entry in p['entries']:
        if entry['resource_type'] == 'Observation':
            print('patient_id:', p['patient_id'])
            print('Observation keys:', list(entry.keys()))
            print('Full entry:', entry)
            break
    else:
        continue
    break
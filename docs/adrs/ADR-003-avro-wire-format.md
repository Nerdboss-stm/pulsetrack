# ADR-003: Avro + Confluent Wire Format for Kafka Payloads

## Status
Accepted

## Date
2026-04-07

## Context

Streaming sources (wearable simulator, pharmacy event simulator, WHOOP poll
bridge) publish to Kafka topics that Spark Structured Streaming consumes.
The on-wire serialization format directly affects:

- **Schema enforcement** — can we reject a producer that drifts from the
  agreed contract?
- **Schema evolution** — can we add a field without breaking old consumers?
- **Spark consumer ergonomics** — does the consumer need bespoke Python or
  can it call a native function?
- **Wire size** — vitals readings are high-volume; payload bloat compounds.
- **Schema registry integration** — does the format play well with Confluent
  Schema Registry / AWS Glue Schema Registry?

Three credible options were on the table: JSON, Protocol Buffers, and Avro.

## Decision

Use **Avro encoded with the Confluent wire format**:

```
┌──────────┬─────────────┬─────────────────────┐
│ 0x00     │ 4-byte      │ Avro binary payload │
│ magic    │ schema ID   │                     │
│ byte     │ (big-endian)│                     │
└──────────┴─────────────┴─────────────────────┘
```

- 1 magic byte (`0x00` — Confluent v0).
- 4-byte big-endian schema ID (registered in Schema Registry).
- Avro binary payload conforming to that schema.

Schemas live in `/Users/nerdboss-stm/pulsetrack-cm/schemas/*.avsc` (committed
to git) and are registered with Confluent Schema Registry (local dev) /
AWS Glue Schema Registry (cloud) at producer startup.

Spark consumers strip the 5-byte prefix via
`F.expr("substring(value, 6, length(value) - 5)")` and decode via
`from_avro(payload, schema_str, {"mode": "PERMISSIVE"})` — see
`streaming/bronze_ingestion.py::_decode_envelope`.

## Consequences

**Positive**:
- Schema enforcement at the producer: the producer can't publish a payload
  that doesn't match a registered schema.
- Schema evolution is first-class: adding a nullable field (e.g. `source_type`
  enum, commit `0b73a4f`) is backwards-compatible without consumer changes.
- Wire size ~5–10× smaller than equivalent JSON. At ~50 vitals/sec/device
  scale, this is real money on Kafka storage + MSK throughput.
- `spark-avro` `from_avro` is native — no Python UDF tax on the hot path.
- Confluent wire format is the de-facto standard. Any Kafka tool (kafkacat,
  Conduktor, KCat) understands it.

**Negative**:
- Not human-readable. `kafka-console-consumer` outputs garbage without a
  Schema-Registry-aware deserializer.
- Requires Schema Registry as an additional service. Local dev runs Confluent
  Schema Registry in Docker; cloud uses AWS Glue Schema Registry.
- A producer that fabricates the wire prefix (or omits it) lands in the DLQ.
  This is a feature, not a bug — but it's worth documenting that the bronze
  pipeline's `is_parseable` flag is the canary for "someone is producing
  malformed Avro."

## Alternatives Considered

- **JSON**: rejected. No schema enforcement at the wire level (a producer
  can ship any shape). Wire size is 5–10× Avro's. Schema evolution lives in
  consumer code, which is exactly the failure mode Avro removes. The only
  thing JSON wins on is debugger-friendliness in `kafkacat`, which is solved
  in Avro with the Schema-Registry-aware variant.
- **Protocol Buffers**: rejected. Spark `from_protobuf` is functional but
  less ecosystem-rich than `from_avro`. Schema evolution rules are stricter
  (no removing fields with the same tag, etc.) and a worse fit for
  iterative schema work. The Confluent ecosystem also leans Avro-first,
  which matters for our future integrations.
- **MessagePack / CBOR**: rejected. No schema enforcement layer. Equivalent
  in size to Avro but lacks the Schema Registry integration story.
- **Raw Avro (no Confluent prefix)**: rejected. Without a schema ID in the
  message, the consumer has to know the schema out-of-band. Confluent's
  prefix is what makes producer/consumer schema evolution actually work in
  practice.

## References

- `/Users/nerdboss-stm/pulsetrack-cm/schemas/sensor_reading.avsc` — wearable
  vitals schema.
- `/Users/nerdboss-stm/pulsetrack-cm/schemas/pharmacy_event.avsc` — pharmacy
  event schema.
- `/Users/nerdboss-stm/pulsetrack-cm/schemas/registry.py` — schema loader.
- `/Users/nerdboss-stm/pulsetrack-cm/streaming/bronze_ingestion.py` —
  `_decode_envelope` strips the prefix and calls `from_avro`.
- `/Users/nerdboss-stm/pulsetrack-cm/data_generators/wearable_generator.py`
  — producer that builds the Confluent-prefixed wire payload.
- `/Users/nerdboss-stm/pulsetrack-cm/scripts/produce_sensor_records.py` —
  test-harness producer.
- Commit `0b73a4f` — `source_type` field added as a backwards-compatible
  enum evolution.
- Related: ADR-005 (streaming-first hybrid).

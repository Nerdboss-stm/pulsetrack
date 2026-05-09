"""Kafka client config helpers — MSK IAM auth.

The same SASL/SSL + IAM auth fields are needed by every producer/consumer
that connects to MSK. Centralizing here keeps drift down and keeps the
``IAMClientCallbackHandler`` class name from being copy-pasted into multiple
files (where deep indentation easily pushes it over the 100-char line limit).

Three callers, three key shapes:
  * Java Kafka client (used internally by Spark): unprefixed keys like
    ``security.protocol``. ``MSK_SASL_CONFIG`` below.
  * Spark's Kafka source/sink: same keys but **prefixed with ``kafka.``** so
    Spark passes them through to the underlying Java client. See
    ``spark_msk_iam_options()``.
  * confluent-kafka-python (librdkafka): does NOT support
    ``sasl.mechanism=AWS_MSK_IAM`` at all — only OAUTHBEARER with an
    ``oauth_cb`` provider. Use ``aws_msk_iam_sasl_signer.MSKAuthTokenProvider``
    directly (see ``scripts/produce_sensor_records.py`` for an example) — do
    NOT use this module for Python producers.
"""

from config import settings

MSK_SASL_CONFIG = {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "AWS_MSK_IAM",
    "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "sasl.client.callback.handler.class": ("software.amazon.msk.auth.iam.IAMClientCallbackHandler"),
}


def apply_msk_auth(config: dict) -> None:
    """If cloud SASL_SSL is configured, merge MSK IAM auth fields into ``config`` in place.

    Java-shape keys. For Spark readStream/writeStream, use
    ``spark_msk_iam_options()`` instead.
    """
    if settings.kafka_security_protocol == "SASL_SSL":
        config.update(MSK_SASL_CONFIG)


def spark_msk_iam_options() -> dict:
    """Return ``kafka.``-prefixed MSK IAM auth options for Spark Kafka source/sink.

    Spark forwards every option whose key starts with ``kafka.`` to the
    underlying Java Kafka client, stripping the prefix. Returns an empty
    dict when running in PLAINTEXT mode (local dev) so the caller can
    unconditionally splat ``**spark_msk_iam_options()`` into ``.options(...)``.
    """
    if settings.kafka_security_protocol != "SASL_SSL":
        return {}
    return {f"kafka.{key}": value for key, value in MSK_SASL_CONFIG.items()}

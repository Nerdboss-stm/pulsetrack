"""Kafka client config helpers — MSK IAM auth.

The same SASL/SSL + IAM auth fields are needed by every producer/consumer
that connects to MSK. Centralizing here keeps drift down and keeps the
``IAMClientCallbackHandler`` class name from being copy-pasted into multiple
files (where deep indentation easily pushes it over the 100-char line limit).
"""

from config import settings

MSK_SASL_CONFIG = {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "AWS_MSK_IAM",
    "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "sasl.client.callback.handler.class": ("software.amazon.msk.auth.iam.IAMClientCallbackHandler"),
}


def apply_msk_auth(config: dict) -> None:
    """If cloud SASL_SSL is configured, merge MSK IAM auth fields into ``config`` in place."""
    if settings.kafka_security_protocol == "SASL_SSL":
        config.update(MSK_SASL_CONFIG)

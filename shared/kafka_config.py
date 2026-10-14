"""Common kafka-python settings for managed SASL/TLS brokers and local Kafka."""
import os


def kafka_client_config() -> dict:
    protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    if protocol == "PLAINTEXT":
        return {}
    config = {"security_protocol": protocol}
    mechanism = os.environ.get("KAFKA_SASL_MECHANISM")
    username = os.environ.get("KAFKA_SASL_USERNAME")
    password = os.environ.get("KAFKA_SASL_PASSWORD")
    if mechanism:
        config["sasl_mechanism"] = mechanism
    if username:
        config["sasl_plain_username"] = username
    if password:
        config["sasl_plain_password"] = password
    return config

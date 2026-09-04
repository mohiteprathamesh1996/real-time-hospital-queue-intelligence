import json

from confluent_kafka import Producer

from simulator.event_models import HospitalEvent


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "hospital-events"


class HospitalEventProducer:
    """Publishes HospitalEvent objects to Kafka/Redpanda."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
    ):
        self.topic = topic

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
            }
        )

    def publish(self, event: HospitalEvent) -> None:
        """Publish one hospital event as JSON."""

        payload = event.model_dump(mode="json")

        self.producer.produce(
            topic=self.topic,
            key=event.event_id,
            value=json.dumps(payload),
        )

        self.producer.poll(0)

    def flush(self) -> None:
        """Wait for all buffered messages to be delivered."""

        remaining = self.producer.flush()

        if remaining > 0:
            raise RuntimeError(
                f"{remaining} message(s) were not delivered"
            )
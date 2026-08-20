from __future__ import annotations

from job_visibility.model import Event
from job_visibility.outbox import BrokerProducer, canonical_edr


class KafkaEdrIngress:
    def __init__(self, producer: BrokerProducer, *, topic: str) -> None:
        self.producer, self.topic = producer, topic

    def publish(self, event: Event) -> None:
        payload, _ = canonical_edr(event)
        self.producer.publish(topic=self.topic, key=event.job_id, value=payload)

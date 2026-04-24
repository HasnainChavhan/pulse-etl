"""
PulseETL — Apache Kafka Consumer
Real-time event stream ingestion at 500K+ records/day.
"""
import asyncio
import json
import logging
from typing import AsyncGenerator, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException
from app.core.config import settings

logger = logging.getLogger(__name__)


class KafkaEventConsumer:
    """
    Async Kafka consumer for high-volume event stream ingestion.

    Consumes from Apache Kafka at 500K+ records/day.
    Yields batches of raw records to the transformation pipeline.
    """

    def __init__(self, batch_size: Optional[int] = None):
        self.batch_size = batch_size or settings.kafka_batch_size
        self._consumer: Optional[Consumer] = None
        self._running = False

    def _create_consumer(self) -> Consumer:
        return Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "auto.offset.reset": settings.kafka_auto_offset_reset,
            "enable.auto.commit": False,  # Manual commit for delivery guarantee
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 30000,
        })

    async def connect(self):
        """Connect to Kafka and subscribe to the events topic."""
        self._consumer = self._create_consumer()
        self._consumer.subscribe([settings.kafka_topic])
        self._running = True
        logger.info(
            f"Kafka consumer connected to {settings.kafka_bootstrap_servers}, "
            f"topic={settings.kafka_topic}, group={settings.kafka_consumer_group}"
        )

    async def disconnect(self):
        """Gracefully shut down the consumer."""
        self._running = False
        if self._consumer:
            self._consumer.close()
            logger.info("Kafka consumer disconnected")

    async def consume_batches(self) -> AsyncGenerator[list[dict], None]:
        """
        Yield batches of raw events from the Kafka topic.

        Batches records up to batch_size before yielding to allow
        efficient bulk processing in the transformation pipeline.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not connected — call connect() first")

        batch = []

        while self._running:
            # Non-blocking poll (run in thread pool to avoid blocking the event loop)
            msg = await asyncio.get_event_loop().run_in_executor(
                None, self._consumer.poll, 0.5
            )

            if msg is None:
                if batch:
                    yield batch
                    self._consumer.commit()
                    batch = []
                await asyncio.sleep(0.01)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                record = json.loads(msg.value().decode("utf-8"))
                record["_kafka_offset"] = msg.offset()
                record["_kafka_partition"] = msg.partition()
                record["_kafka_timestamp"] = msg.timestamp()[1]
                batch.append(record)
            except json.JSONDecodeError:
                logger.warning(f"Skipping non-JSON message at offset {msg.offset()}")

            if len(batch) >= self.batch_size:
                yield batch
                self._consumer.commit()
                batch = []

    def stop(self):
        """Signal the consumer to stop after the current batch."""
        self._running = False

"""Thin wrapper around confluent-kafka.

The relay is the only producer in the system (see "The relay — transport,
never a router"); everything else that needs to "publish" writes an outbox
row instead. This module also owns topic creation, since every topic in
"Complete topic list" needs to exist before anything runs.
"""

from __future__ import annotations

import json
import logging
import random
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from docpipeline import config

log = logging.getLogger(__name__)


def ensure_topics(topics: list[str] | None = None, partitions: int | None = None) -> None:
    topics = topics or config.TOPICS
    partitions = partitions or config.TOPIC_PARTITIONS
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=10).topics
    to_create = [t for t in topics if t not in existing]
    if not to_create:
        return
    futures = admin.create_topics(
        [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in to_create]
    )
    for topic, fut in futures.items():
        try:
            fut.result()
            log.info("created topic %s", topic)
        except KafkaException as exc:
            # already-exists races are fine; anything else is real
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})


def make_consumer(group_id: str, topics: list[str]) -> Consumer:
    # See config.py's KAFKA_JOIN_JITTER_SECONDS -- staggers simultaneous
    # joins from a KEDA-scaled Deployment's replicas, all of which restart at
    # the same instant on a rollout and would otherwise hit the broker's
    # JoinGroup/SyncGroup at nearly the same time.
    if config.KAFKA_JOIN_JITTER_SECONDS > 0:
        time.sleep(random.uniform(0, config.KAFKA_JOIN_JITTER_SECONDS))
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": config.KAFKA_MAX_POLL_INTERVAL_MS,
    }
    if config.KAFKA_CONSUMER_DEBUG:
        conf["debug"] = "consumer,cgrp"
    c = Consumer(conf)
    c.subscribe(topics)
    return c


def publish(
    producer: Producer,
    topic: str,
    payload: dict,
    headers: dict | None = None,
    key: str | None = None,
    on_delivery=None,
) -> None:
    """Enqueue one message. Delivery is asynchronous — `produce()` returning
    means librdkafka accepted it into its local queue, *not* that the broker
    has it. Callers that must not lose the message (the outbox relay) pass
    `on_delivery` and check `flush()`'s return value; without both, a failed
    delivery is silently invisible.
    """
    kafka_headers = [(k, str(v).encode()) for k, v in (headers or {}).items()]
    producer.produce(
        topic,
        key=(key or payload.get("doc_id", "")).encode(),
        value=json.dumps(payload).encode(),
        headers=kafka_headers or None,
        on_delivery=on_delivery,
    )
    producer.poll(0)


def poll_json(consumer: Consumer, timeout: float = 1.0) -> tuple[dict, object] | tuple[None, None]:
    """Returns (payload, message) or (None, None) on timeout. Caller commits."""
    msg = consumer.poll(timeout)
    if msg is None:
        return None, None
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            return None, None
        raise KafkaException(msg.error())
    return json.loads(msg.value()), msg

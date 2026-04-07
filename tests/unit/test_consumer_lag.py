"""Both consumers must ask for committed offsets one partition at a time.

`KafkaConsumer.committed()` takes a single TopicPartition; passing the whole
assignment (as both consumers did) raises TypeError on the first poll and kills
the consumer.
"""

from __future__ import annotations

import pytest
from kafka.structs import TopicPartition

import ingestion.lake_consumer as lake
import ingestion.predict_consumer as predict


class StrictConsumer:
    """Mirrors kafka-python's committed(partition) signature."""

    def __init__(self, end_offset: int = 10, committed_offset: int = 4):
        self.tp = TopicPartition("transactions_raw", 0)
        self._end = end_offset
        self._committed = committed_offset

    def assignment(self):
        return {self.tp}

    def end_offsets(self, partitions):
        return {tp: self._end for tp in partitions}

    def committed(self, partition):
        if not isinstance(partition, TopicPartition):
            raise TypeError("partition must be a TopicPartition namedtuple")
        return self._committed


@pytest.mark.parametrize("module", [lake, predict], ids=["lake", "predict"])
def test_refresh_lag_queries_committed_per_partition(module):
    module._refresh_lag(StrictConsumer())
    assert module.REGISTRY.get_sample_value("kafka_consumer_lag") == 6


@pytest.mark.parametrize("module", [lake, predict], ids=["lake", "predict"])
def test_refresh_lag_ignores_unassigned_consumer(module):
    class Idle:
        def assignment(self):
            return set()

        def end_offsets(self, partitions):  # pragma: no cover - must not run
            raise AssertionError("end_offsets called without an assignment")

    module._refresh_lag(Idle())

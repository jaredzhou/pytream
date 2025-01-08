from datetime import datetime
import socket
import uuid
import logging
from ..core.event import Event, EventCollector
from ..core.source import Source
from ..core.operator import Operator
from ..core.grouping_strategy import GroupingStrategy, FieldGrouping
from ..core.job import Job
from ..core.stream import Streams
from ..core.engine.stream_engine import StreamEngine
from ..core.engine.instance_executor import InstanceExecutor
from typing import override

logger = logging.getLogger(__name__)


class TxEvent(Event):
    def __init__(
        self,
        transaction_id: str,
        amount: float,
        transaction_time: datetime,
        merchandise_id: int,
        user_account: int,
    ):
        self.transaction_id = transaction_id
        self.amount = amount
        self.transaction_time = transaction_time
        self.merchandise_id = merchandise_id
        self.user_account = user_account

    @override
    def __str__(self) -> str:
        return f"TxEvent(transaction_id={self.transaction_id}, amount={self.amount}, transaction_time={self.transaction_time}, merchandise_id={self.merchandise_id}, user_account={self.user_account})"


class TxScoreEvent(Event):
    def __init__(self, transaction: TxEvent, score: float):
        self.transaction = transaction
        self.score = score

    @override
    def __str__(self) -> str:
        return f"TxScoreEvent(transaction={self.transaction}, score={self.score})"


class TxSource(Source):
    def __init__(self, name: str, parallelism: int, port: int):
        super().__init__(name, parallelism)
        self.port = port

    @override
    def setup_instance(self, instance: int):
        self.instance = instance
        self.reader = self.setup_socket_reader(self.port + instance)

    @override
    def get_events(self, event_collector: EventCollector):
        try:
            transaction = self.reader.readline().decode("utf-8").strip()
            if transaction is None:
                return

            amount, merchandise_id = transaction.split(",")
            amount = float(amount)
            merchandise_id = int(merchandise_id)

            transaction_id = str(uuid.uuid4())
            transaction_time = datetime.now()
            user_account = 1

            event_collector.add(
                TxEvent(
                    transaction_id,
                    amount,
                    transaction_time,
                    merchandise_id,
                    user_account,
                )
            )
        except Exception as e:
            print(f"Error reading transaction: {e}")

    def setup_socket_reader(self, port: int):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        return sock.makefile("rwb")


class GroupByTransactionId(FieldGrouping):
    def __init__(self):
        super().__init__()

    @override
    def get_key(self, event: Event) -> str:
        assert isinstance(event, TxScoreEvent)
        return event.transaction.transaction_id


class UserAccountGrouping(FieldGrouping):
    def __init__(self):
        super().__init__()

    @override
    def get_key(self, event: Event) -> str:
        assert isinstance(event, TxEvent)
        return event.user_account


class AvgTicketAnalyzer(Operator):
    def __init__(self, name: str, parallelism: int, grouping: GroupingStrategy):
        super().__init__(name, parallelism, grouping)

    @override
    def setup_instance(self, instance: int):
        super().setup_instance(instance)
        self.instance = instance

    @override
    def apply(self, event: Event, collector: EventCollector):
        assert isinstance(event, TxEvent)
        collector.add(TxScoreEvent(event, 0.0))


class WindowedProximityAnalyzer(Operator):
    def __init__(self, name: str, parallelism: int, grouping: GroupingStrategy):
        super().__init__(name, parallelism, grouping)

    @override
    def setup_instance(self, instance: int):
        super().setup_instance(instance)
        self.instance = instance

    @override
    def apply(self, event: Event, collector: EventCollector):
        assert isinstance(event, TxEvent)
        collector.add(TxScoreEvent(event, 0.0))


class ScoreStorage:
    def __init__(self):
        self.scores = {}

    def get(self, transaction_id: str, default: float) -> float:
        return self.scores.get(transaction_id, default)

    def put(self, transaction_id: str, score: float):
        self.scores[transaction_id] = score


class WindowedTransactionCountAnalyzer(Operator):
    def __init__(self, name: str, parallelism: int, grouping: GroupingStrategy):
        super().__init__(name, parallelism, grouping)

    @override
    def setup_instance(self, instance: int):
        super().setup_instance(instance)
        self.instance = instance

    @override
    def apply(self, event: Event, collector: EventCollector):
        assert isinstance(event, TxEvent)
        collector.add(TxScoreEvent(event, 0.0))


class ScoreAggregator(Operator):
    def __init__(
        self,
        name: str,
        parallelism: int,
        grouping: GroupingStrategy,
        store: ScoreStorage,
    ):
        super().__init__(name, parallelism, grouping)
        self.store = store

    @override
    def setup_instance(self, instance: int):
        super().setup_instance(instance)
        self.instance = instance

    @override
    def apply(self, event: Event, collector: EventCollector):
        assert isinstance(event, TxScoreEvent)
        old_score = self.store.get(event.transaction.transaction_id, 0)
        self.store.put(event.transaction.transaction_id, old_score + event.score)
        print(f"Score: {self.store.scores}")


def main():
    job = Job("fraud_detection_job")
    txOut = job.add_source(TxSource("tx_source", 1, 9990))
    eval_results1 = txOut.apply_operator(
        AvgTicketAnalyzer("avg_ticket_analyzer", 2, UserAccountGrouping())
    )
    eval_results2 = txOut.apply_operator(
        WindowedProximityAnalyzer(
            "windowed_proximity_analyzer", 2, UserAccountGrouping()
        )
    )
    eval_results3 = txOut.apply_operator(
        WindowedTransactionCountAnalyzer(
            "windowed_transaction_count_analyzer", 2, UserAccountGrouping()
        )
    )

    store = ScoreStorage()
    Streams.of(eval_results1, eval_results2, eval_results3).apply_operator(
        ScoreAggregator("score_aggregator", 2, GroupByTransactionId(), store)
    )

    logger.info(
        "This is a streaming job that detect suspicious transactions."
        "Input needs to be in this format: {amount},{merchandiseId}. For example: 42.00,3."
        "Merchandises N and N + 1 are 1 seconds walking distance away from each other."
    )

    engine = StreamEngine()
    engine.submit(job)


if __name__ == "__main__":
    main()

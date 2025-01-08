from typing import override
from ..core.operator import Operator
from ..core.event import Event, EventCollector
from ..core.grouping_strategy import GroupingStrategy
from ..core.job import Job
from .fraud import TxSource as TransactionSource
from ..core.engine.stream_engine import StreamEngine
from .fraud import FieldGrouping
from .fraud import TxScoreEvent, TxEvent


class UsageEvent(Event):
    def __init__(self, transaction_count: int, fraud_transaction_count: int):
        self.transaction_count = transaction_count
        self.fraud_transaction_count = fraud_transaction_count


class SystemUsageAnalyzer(Operator):
    def __init__(self, name: str, parallelism: int, grouping: GroupingStrategy):
        super().__init__(name, parallelism, grouping)
        self.transaction_count = 0
        self.fraud_transaction_count = 0
        self.instance = 0

    @override
    def setup_instance(self, instance: int):
        self.instance = instance

    @override
    def apply(self, event: Event, collector: EventCollector):
        self.transaction_count += 1
        collector.add(UsageEvent(self.transaction_count, self.fraud_transaction_count))


class UsageWriter(Operator):
    def __init__(self, name: str, parallelism: int, grouping: GroupingStrategy):
        super().__init__(name, parallelism, grouping)
        self.instance = 0

    @override
    def setup_instance(self, instance: int):
        self.instance = instance

    @override
    def apply(self, event: Event, collector: EventCollector):
        print(event)


class TransactionFieldsGrouping(FieldGrouping):
    def __init__(self):
        super().__init__()

    def get_key(self, event: Event) -> str:
        if isinstance(event, TxEvent):
            return str(event.user_account)
        elif isinstance(event, UsageEvent):
            return str(event.transaction_count)
        else:
            raise ValueError(f"未知的事件类型: {type(event)}")


def main():
    import sys

    print(sys.path)
    job = Job("system_usage_job")
    job.add_source(TransactionSource("transaction source", 1, 9990)).apply_operator(
        SystemUsageAnalyzer("system usage analyzer", 2, TransactionFieldsGrouping())
    ).apply_operator(UsageWriter("usage writer", 2, TransactionFieldsGrouping()))

    engine = StreamEngine()
    engine.submit(job)


if __name__ == "__main__":
    main()

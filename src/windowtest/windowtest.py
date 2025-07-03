from datetime import datetime
import socket
import uuid
from typing import List, override, Any
from ..core.job import Job
from ..core.source import Source
from ..core.event import Event, TimedEvent, EventCollector
from ..core.window import FixedTimeWindowingStrategy, WindowOperator, EventWindow
from ..core.engine.stream_engine import StreamEngine
from ..core.grouping_strategy import GroupingStrategy, FieldGrouping


class TxEvent(TimedEvent):
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
    def get_time(self) -> int:
        return int(self.transaction_time.timestamp() * 1000)

    @override
    def __str__(self) -> str:
        return f"TxEvent(transaction_id={self.transaction_id}, amount={self.amount}, transaction_time={self.transaction_time}, merchandise_id={self.merchandise_id}, user_account={self.user_account})"


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


class TestWindowedAnalyzer(WindowOperator):
    def __init__(self, name: str, num_instances: int, grouping: GroupingStrategy):
        super().__init__(name, num_instances, grouping)

    @override
    def apply_window(self, window: EventWindow, collector: List[Event]) -> None:
        print(
            f"{len(window.get_events())} transactions are received between {window.get_start_time()} and {window.get_end_time()}"
        )
        for event in window.get_events():
            print(f"Event: {event}")

    @override
    def setup_instance(self, instance: int) -> None:
        return super().setup_instance(instance)


class UserAccountFieldsGrouping(FieldGrouping):
    @override
    def get_key(self, event: Event) -> Any:
        assert isinstance(event, TxEvent)
        return event.user_account

    @override
    def __str__(self) -> str:
        return "UserAccountFieldsGrouping"


def main():
    job = Job("windowing_test_job")

    job.add_source(TxSource("transaction source", 1, 9990)).with_windowing(
        FixedTimeWindowingStrategy(10000, 2000)
    ).apply_operator(
        TestWindowedAnalyzer("test windowed analyzer", 2, UserAccountFieldsGrouping())
    )

    print(
        "This is a streaming job that works with a windowed strategy and a windowed operator."
        + "Input needs to be in this format: {amount},{merchandiseId}. For example: 42.00@3."
    )

    engine = StreamEngine()
    engine.submit(job)


if __name__ == "__main__":
    main()

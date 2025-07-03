from typing import cast
from .process import Process
from .event_queue import EventQueue
from ..core import Operator
from ..grouping_strategy import GroupingStrategy


class DispatchExecutor(Process):
    """事件分发执行器，负责将事件分发给下游组的实例"""

    def __init__(self, downstream_executor):
        # 延迟导入以避免循环引用
        from .component_executor import ComponentExecutor

        super().__init__()
        assert isinstance(downstream_executor, ComponentExecutor)
        self.downstream_executor = downstream_executor
        self.incoming_queue: EventQueue = None

    def run_once(self) -> bool:
        """执行一次事件分发"""
        try:
            # 获取事件
            event = self.incoming_queue.take()

            # 获取分组策略 (转换为 Operator 类型)
            operator = cast(Operator, self.downstream_executor.component)
            print(f"\nOperator: {operator.__class__.__name__}")
            strategy = operator.get_grouping_strategy(
                self.incoming_queue.get_stream_name()
            )
            print(f"\nStrategy: {strategy.__class__.__name__}")
            # 计算目标实例
            instance_id = strategy.get_instance(
                event, len(self.downstream_executor.instance_executors)
            )
            if instance_id == GroupingStrategy.ALL_INSTANCES:
                for instance_executor in self.downstream_executor.instance_executors:
                    instance_executor.get_incoming_queue().put(event)
            else:
                # 获取目标实例的输入队列
                target_queue = self.downstream_executor.instance_executors[
                    instance_id
                ].get_incoming_queue()
                # 发送事件
                target_queue.put(event)
            return True

        except Exception as e:
            print(f"Error in dispatch process: {e}")
            print(f"Event: {event}")
            print(f"Strategy: {strategy}")
            print(
                f"Number of instances: {len(self.downstream_executor.instance_executors)}"
            )
            raise

    def set_incoming_queue(self, queue: EventQueue) -> None:
        """设置输入队列"""
        self.incoming_queue = queue

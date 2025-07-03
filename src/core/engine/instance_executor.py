from typing import List, Dict, Set
from .process import Process
from ..event import Event, EventCollector
from .event_queue import EventQueue
from ..core import Source, Operator


class InstanceExecutor(Process):
    """实例执行器"""

    def __init__(self, instance_id: int):
        super().__init__()
        self.instance_id = instance_id
        self.event_collector = EventCollector()
        self.incoming_queue: EventQueue = None
        # 每个通道对应多个输出队列
        self.outgoing_queues: Dict[str, List[EventQueue]] = {}

    def register_channel(self, channel: str) -> None:
        """注册新的通道"""
        self.event_collector.register_channel(channel)
        self.outgoing_queues[channel] = []

    def add_outgoing_queue(
        self, queue: EventQueue, channel: str = EventCollector.DEFAULT_CHANNEL
    ) -> None:
        """添加输出队列到指定通道"""
        if channel not in self.outgoing_queues:
            self.register_channel(channel)
        self.outgoing_queues[channel].append(queue)

    def set_incoming_queue(self, queue: EventQueue) -> None:
        self.incoming_queue = queue

    def get_incoming_queue(self) -> EventQueue:
        return self.incoming_queue


class SourceInstanceExecutor(InstanceExecutor):
    def __init__(self, instance_id: int, source: Source):
        super().__init__(instance_id)
        self.source = source
        self.source.setup_instance(instance_id)

    def run_once(self) -> bool:
        try:
            # 清空收集器
            self.event_collector.clear()

            # 获取事件
            self.source.get_events(self.event_collector)

            # 发送到下游
            for channel in self.event_collector.get_registered_channels():
                events = self.event_collector.get_event_list(channel)
                queues = self.outgoing_queues.get(channel, [])
                for event in events:
                    for queue in queues:
                        queue.put(event)
            return True
        except Exception as e:
            print(f"Error in source instance: {e}")
            return False


class OperatorInstanceExecutor(InstanceExecutor):
    def __init__(self, instance_id: int, operator: Operator):
        super().__init__(instance_id)
        self.operator = operator
        self.operator.setup_instance(instance_id)

    def run_once(self) -> bool:
        try:
            # 获取输入
            event = self.incoming_queue.take()

            # 清空收集器
            self.event_collector.clear()

            # 处理事件
            self.operator.apply(event, self.event_collector)

            # 发送到下游
            for channel in self.event_collector.get_registered_channels():
                events = self.event_collector.get_event_list(channel)
                queues = self.outgoing_queues.get(channel, [])
                for event in events:
                    for queue in queues:
                        queue.put(event)
            return True
        except Exception as e:
            print(f"Error in operator instance: {e}")
            return False

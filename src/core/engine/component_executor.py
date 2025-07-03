from typing import List
from copy import deepcopy
from ..core import Source, Component
from .instance_executor import (
    InstanceExecutor,
    SourceInstanceExecutor,
    OperatorInstanceExecutor,
    EventCollector,
)
from .event_queue import EventQueue
from .dispatch_executor import DispatchExecutor


class ComponentExecutor:
    """执行器组，管理组件的实例执行器和分发执行器"""

    def __init__(self, component: Component, queue_size: int = 64):
        self.component = component
        self.instance_executors: List[InstanceExecutor] = []
        self.dispatch_executor = None
        self.incoming_queues: List[EventQueue] = []
        self.queue_size = queue_size

        # 创建实例执行器
        parallelism = component.get_parallelism()
        for i in range(parallelism):
            # 为每个实例克隆组件
            instance_component = deepcopy(component)
            # 根据组件类型创建对应的实例执行器
            if isinstance(component, Source):
                instance = SourceInstanceExecutor(i, instance_component)
            else:
                instance = OperatorInstanceExecutor(i, instance_component)
                # 为每个算子实例创建输入队列
            self.instance_executors.append(instance)

        # 如果不是Source，创建分发执行器
        if not isinstance(component, Source):
            self.dispatch_executor = DispatchExecutor(self)

    def start(self):
        """启动所有执行器"""
        # 启动实例执行器
        for executor in self.instance_executors:
            executor.start()

        # 启动分发执行器
        if self.dispatch_executor:
            self.dispatch_executor.start()

    def set_incoming_queue(self, queues: List[EventQueue]) -> None:
        """设置输入队列到分发器，并为每个实例初始化输入队列"""
        if isinstance(self.component, Source):
            raise RuntimeError("数据源执行器不允许设置输入队列")

        # 设置分发器的输入队列
        self.incoming_queues = queues
        self.dispatch_executor.set_incoming_queue(queue)

        # 为每个实例创建输入队列
        for instance in self.instance_executors:
            q = EventQueue(self.queue_size, queue.get_stream_name())
            instance.set_incoming_queue(q)

    def register_channel(self, channel: str) -> None:
        """注册新的通道"""
        for instance in self.instance_executors:
            instance.register_channel(channel)

    def add_outgoing_queue(
        self, queue: EventQueue, channel: str = EventCollector.DEFAULT_CHANNEL
    ) -> None:
        """添加输出队列到指定通道"""
        for instance in self.instance_executors:
            instance.add_outgoing_queue(queue, channel)

    def get_instance_executor_incoming_queue(self, index: int) -> EventQueue:
        """获取实例执行器的输入队列"""
        return self.instance_executors[index].get_incoming_queue()

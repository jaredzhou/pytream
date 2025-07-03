from typing import List, Dict
from ..job import Job
from ..core import Operator, Component
from .component_executor import ComponentExecutor
from .dispatch_executor import DispatchExecutor
from .event_queue import EventQueue


class Connection:
    def __init__(
        self,
        source: ComponentExecutor,
        target: ComponentExecutor,
        channel: str,
        stream_name: str,
    ):
        self.source = source
        self.target = target
        self.channel = channel
        self.stream_name = stream_name


class StreamEngine:
    """流处理执行引擎"""

    QUEUE_SIZE = 64

    def __init__(self):
        self.component_executors: List[ComponentExecutor] = []
        self.operator_map: Dict[Operator, ComponentExecutor] = {}
        self.connections: List[Connection] = []

    def submit(self, job: Job) -> None:
        """提交作业

        Args:
            job: 要执行的作业
        """
        self._setup_executors(job)
        self._setup_connections()
        self._start_processes()

    def _setup_executors(self, job: Job) -> None:
        """设置所有组件的执行器组"""
        for source in job.get_sources():
            executor = ComponentExecutor(source)
            self.component_executors.append(executor)
            self._traverse_component(source, executor)

    def _setup_connections(self) -> None:
        """设置执行器组之间的连接"""
        # for executor in self.component_executors:
        #     downstream_operators = executor.component.get_downstream_operators()
        #     if downstream_operators:
        #         for operator in downstream_operators:
        #             downstream = self.operator_map.get(operator)
        #             if downstream:
        #                 # 创建队列
        #                 queue = EventQueue(self.QUEUE_SIZE)
        #                 # 设置上游组的输出队列和下游组的输入队列
        #                 executor.set_outgoing_queue(queue)
        #                 downstream.set_incoming_queue(queue)
        for connection in self.connections:
            queue = EventQueue(self.QUEUE_SIZE, connection.stream_name)
            connection.source.register_channel(connection.channel)
            connection.source.add_outgoing_queue(queue, connection.channel)
            connection.target.set_incoming_queue(queue)

    def _start_processes(self) -> None:
        """启动所有进程"""
        for executor in self.component_executors:
            executor.start()

    def _traverse_component(
        self, component: Component, executor: ComponentExecutor
    ) -> None:
        """遍历组件的下游算子"""
        stream = component.get_outgoing_stream()

        # 遍历所有通道
        for channel in stream.get_channels():
            # 获取该通道的所有算子
            operators = stream.get_applied_operators(channel)
            # 遍历算子字典
            for stream_name, operator in operators.items():
                print(f"stream: {stream_name}, operator: {operator.name}")
                if operator not in self.operator_map:
                    operator_executor = ComponentExecutor(operator, self.QUEUE_SIZE)
                    self.operator_map[operator] = operator_executor
                    self.component_executors.append(operator_executor)
                    # 递归遍历下游
                    self._traverse_component(operator, operator_executor)
                else:
                    operator_executor = self.operator_map[operator]

                self.connections.append(
                    Connection(executor, operator_executor, channel, stream_name)
                )

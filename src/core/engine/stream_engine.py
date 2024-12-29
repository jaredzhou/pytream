from typing import List, Dict
from ..job import Job
from .event_queue import EventQueue
from .component_executor import ComponentExecutor
from ..operator import Operator

class StreamEngine:
    """流处理执行引擎"""
    
    QUEUE_SIZE = 64
    
    def __init__(self):
        self.executors: List[ComponentExecutor] = []
        self.operator_map: Dict[Operator, ComponentExecutor] = {}
        
    def submit(self, job: Job) -> None:
        """提交作业
        
        Args:
            job: 要执行的作业
        """
        # 设置组件执行器
        self._setup_component_executors(job)
        
        # 设置连接
        self._setup_connections()
        
        # 启动所有进程
        self._start_processes()
        
    def _setup_component_executors(self, job: Job) -> None:
        """设置所有组件的执行器"""
        # 从数据源开始遍历
        for source in job.get_sources():
            executor = ComponentExecutor(source)
            self.executors.append(executor)
            self._traverse_component(source, executor)
            
    def _setup_connections(self) -> None:
        """设置执行器之间的连接"""
        for executor in self.executors:
            if executor.component.get_outgoing_stream():
                queue = EventQueue(self.QUEUE_SIZE)
                executor.set_outgoing_queue(queue)
                
                # 连接下游
                for operator in executor.component.get_outgoing_stream().get_operators():
                    downstream = self.operator_map.get(operator)
                    if downstream:
                        downstream.set_incoming_queue(queue)
                        
    def _start_processes(self) -> None:
        """启动所有进程"""
        self.executors.reverse()
        for executor in self.executors:
            executor.start()
            
    def _traverse_component(self, component, executor):
        """遍历组件"""
        stream = component.get_outgoing_stream()
        if not stream:
            return
            
        for operator in stream._operator_set:
            if operator not in self.operator_map:
                operator_executor = ComponentExecutor(operator)
                self.operator_map[operator] = operator_executor
                self.executors.append(operator_executor)
                self._traverse_component(operator, operator_executor)

from abc import ABC, abstractmethod
from typing import List
from .component import Component
from .event import Event

class Operator(Component, ABC):
    """算子基类,用于处理流数据"""
    
    def __init__(self, name: str, parallelism: int = 1):
        """初始化算子
        
        Args:
            name: 算子名称
            parallelism: 并行度
        """
        super().__init__(name, parallelism)
    
    @abstractmethod
    def setup_instance(self, instance: int) -> None:
        """设置算子实例
        
        Args:
            instance: 实例ID(从0开始的索引)
        """
        pass
        
    @abstractmethod
    def apply(self, event: Event, collector: List[Event]) -> None:
        """处理输入事件并生成结果
        
        Args:
            event: 输入事件
            collector: 输出事件收集器
        """
        pass

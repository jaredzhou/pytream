from abc import ABC, abstractmethod
from typing import List
from .component import Component
from .event import Event

class Source(Component, ABC):
    """数据源基类"""
    
    def __init__(self, name: str, parallelism: int = 1):
        """初始化数据源
        
        Args:
            name: 数据源名称
            parallelism: 并行度
        """
        super().__init__(name, parallelism)
    
    @abstractmethod
    def setup_instance(self, instance: int) -> None:
        """设置数据源实例
        
        Args:
            instance: 实例ID(从0开始的索引)
        """
        pass
        
    @abstractmethod
    def get_events(self, collector: List[Event]) -> None:
        """获取事件
        
        Args:
            collector: 事件收集器
        """
        pass

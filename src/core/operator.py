from abc import ABC, abstractmethod
from typing import List
from .component import Component
from .event import Event
from .grouping_strategy import GroupingStrategy, RoundRobinGrouping


class Operator(Component, ABC):
    """算子基类,用于处理流数据"""

    def __init__(
        self,
        name: str,
        parallelism: int = 1,
        grouping: GroupingStrategy = RoundRobinGrouping(),
    ):
        """初始化算子

        Args:
            name: 算子名称
            parallelism: 并行度
            grouping: 分组策略
        """
        super().__init__(name, parallelism)
        self._grouping_strategy = grouping

    def set_grouping_strategy(self, strategy: GroupingStrategy) -> None:
        """设置分组策略

        Args:
            strategy: 分组策略
        """
        self._grouping_strategy = strategy

    def get_grouping_strategy(self) -> GroupingStrategy:
        """获取分组策略"""
        return self._grouping_strategy

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

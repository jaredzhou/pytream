from abc import ABC, abstractmethod
from typing import override

from .event import Event


class GroupingStrategy(ABC):
    """事件分组策略"""

    ALL_INSTANCES = -1  # 特殊值，表示发送到所有实例

    @abstractmethod
    def get_instance(self, event: Event, num_instances: int) -> int:
        """获取事件应该发送到哪个实例

        Args:
            event: 要分发的事件
            num_instances: 实例总数

        Returns:
            目标实例索引
        """
        pass


class RoundRobinGrouping(GroupingStrategy):
    """轮询分组策略"""

    def __init__(self):
        self.current = -1

    @override
    def get_instance(self, event: Event, num_instances: int) -> int:
        self.current = (self.current + 1) % num_instances
        return self.current


class FieldGrouping(GroupingStrategy):
    """字段分组策略"""

    @abstractmethod
    def get_key(self, event: Event) -> str:
        """获取事件字段值"""
        pass

    @override
    def get_instance(self, event: Event, num_instances: int) -> int:
        # 获取字段值并计算哈希
        key = self.get_key(event)
        instance = hash(str(key)) % num_instances
        print(f"Key: {key} -> instance {instance}")
        return instance


class AllGrouping(GroupingStrategy):
    """全部分组策略，将每个事件路由到所有下游实例"""

    @override
    def get_instance(self, event: Event, num_instances: int) -> int:
        """获取目标实例ID

        Args:
            event: 要路由的事件对象
            num_instances: 组件的并行度

        Returns:
            ALL_INSTANCES，表示发送到所有实例
        """
        return self.ALL_INSTANCES

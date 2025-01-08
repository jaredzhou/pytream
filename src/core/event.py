from abc import ABC, abstractmethod
from typing import Any, Dict, List, Set


class Event(ABC):
    """事件基类,代表流中传递的数据单元"""

    def __init__(self, data: Dict[str, Any]):
        """初始化事件

        Args:
            data: 事件数据字典
        """
        self._data = data

    def get_field(self, field: str) -> Any:
        """获取事件字段值

        Args:
            field: 字段名

        Returns:
            字段值
        """
        return self._data.get(field)

    def get_fields(self) -> Dict[str, Any]:
        """获取所有字段

        Returns:
            事件数据字典
        """
        return self._data


class EventCollector:
    """事件收集器，支持多通道"""

    DEFAULT_CHANNEL = "default"

    def __init__(self):
        # 每个通道对应一个事件列表
        self.queues: Dict[str, List[Event]] = {}
        # 已注册的通道集合
        self.registered_channels: Set[str] = set()

    def register_channel(self, channel: str) -> None:
        """注册新的通道"""
        self.registered_channels.add(channel)
        self.queues[channel] = []

    def add(self, event: Event, channel: str = DEFAULT_CHANNEL) -> None:
        """添加事件到指定通道"""
        if channel in self.queues:
            self.queues[channel].append(event)

    def get_registered_channels(self) -> Set[str]:
        """获取已注册的通道集合"""
        return self.registered_channels

    def get_event_list(self, channel: str) -> List[Event]:
        """获取指定通道的事件列表"""
        return self.queues.get(channel, [])

    def clear(self) -> None:
        """清空所有通道的事件"""
        for queue in self.queues.values():
            queue.clear()

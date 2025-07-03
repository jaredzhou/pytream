from queue import Queue
from typing import Optional
from ..event import Event, NamedEvent


class EventQueue:
    """事件队列"""

    def __init__(self, max_size: int = 64, stream_name: Optional[str] = None):
        """初始化队列

        Args:
            max_size: 最大队列大小
        """
        self._stream_name: Optional[str] = stream_name
        self.queue: Queue[NamedEvent] = Queue(maxsize=max_size)

    def get_stream_name(self) -> Optional[str]:
        """获取流名称"""
        return self._stream_name

    def put(self, event: Event) -> None:
        """放入事件

        如果设置了流名称且输入事件不是NamedEvent，
        会自动将其包装为NamedEvent
        """
        if self._stream_name and not isinstance(event, NamedEvent):
            event = NamedEvent(self._stream_name, event.get_fields())
        self.queue.put(event)

    def take(self) -> Optional[Event]:
        """获取事件"""
        return self.queue.get()

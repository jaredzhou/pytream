from queue import Queue
from typing import Optional
from ..event import Event

class EventQueue:
    """事件队列"""
    
    def __init__(self, max_size: int = 64):
        self.queue = Queue(maxsize=max_size)
        
    def put(self, event: Event) -> None:
        """放入事件"""
        self.queue.put(event)
        
    def take(self) -> Optional[Event]:
        """获取事件"""
        return self.queue.get()

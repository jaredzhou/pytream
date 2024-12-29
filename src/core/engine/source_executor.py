from typing import List
from .component_executor import ComponentExecutor
from ..event import Event
from .event_queue import EventQueue

class SourceExecutor(ComponentExecutor):
    """数据源执行器"""
    
    def set_incoming_queue(self, queue: EventQueue) -> None:
        """设置输入队列 (不允许)
        
        Args:
            queue: 事件队列
            
        Raises:
            RuntimeError: 数据源执行器不允许设置输入队列
        """
        raise RuntimeError("数据源执行器不允许设置输入队列")
    
    def run_once(self) -> bool:
        """执行一次数据源处理
        
        Returns:
            是否继续执行
        """
        # 清空收集器
        self.event_collector.clear()
        
        # 从数据源获取事件
        self.component.get_events(self.event_collector)
        
        # 发送到下游
        try:
            for event in self.event_collector:
                if self.outgoing_queue:
                    self.outgoing_queue.put(event)
            return True
        except Exception as e:
            print(f"Error in source executor: {e}")
            return False
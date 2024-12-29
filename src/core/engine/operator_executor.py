from typing import List
from .component_executor import ComponentExecutor
from ..event import Event
from ..operator import Operator

class OperatorExecutor(ComponentExecutor):
    """算子执行器"""
    
    def __init__(self, operator: Operator):
        """初始化算子执行器
        
        Args:
            operator: 要执行的算子
        """
        super().__init__(operator)
        self.operator = operator
        
    def run_once(self) -> bool:
        """执行一次算子处理
        
        Returns:
            是否继续执行
        """
        try:
            # 从输入队列读取事件
            event = self.incoming_queue.take()
            
            # 清空收集器
            self.event_collector.clear()
            
            # 应用算子处理
            self.operator.apply(event, self.event_collector)
            
            # 发送到下游
            for output_event in self.event_collector:
                if self.outgoing_queue:
                    self.outgoing_queue.put(output_event)
                    
            return True
            
        except Exception as e:
            print(f"Error in operator executor: {e}")
            return False 
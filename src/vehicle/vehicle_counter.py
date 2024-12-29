from typing import List, Dict
from ..core.operator import Operator
from ..core.event import Event
from .vehicle_event import VehicleEvent

class VehicleCounter(Operator):
    """车辆计数器算子"""
    
    def __init__(self, name: str):
        super().__init__(name)
        self.type_counts: Dict[str, int] = {}
        
    def setup_instance(self, instance: int) -> None:
        """设置实例"""
        pass
        
    def apply(self, event: Event, collector: List[Event]) -> None:
        """处理车辆事件
        
        Args:
            event: 输入事件
            collector: 输出事件收集器
        """
        # 直接获取车辆类型并更新计数
        vehicle_type = event.get_data()
        self.type_counts[vehicle_type] = self.type_counts.get(vehicle_type, 0) + 1
        
        # 打印各类型计数
        print("Vehicle Types Count:", self.type_counts) 
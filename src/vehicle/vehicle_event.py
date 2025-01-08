from typing import List
import socket
import sys
from ..core.source import Source
from ..core.event import Event


class VehicleEvent(Event):
    """车辆事件"""

    def __init__(self, vehicle_type: str):
        """初始化车辆事件

        Args:
            vehicle_type: 车辆类型
        """
        super().__init__({"type": vehicle_type})
        self.vehicle_type = vehicle_type

    def get_data(self) -> str:
        """获取事件数据

        Returns:
            车辆类型
        """
        return self.vehicle_type

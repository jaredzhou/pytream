from typing import List
import socket
import sys
from ..core.source import Source
from ..core.event import Event


class SensorReader(Source):
    """从socket读取车辆数据的传感器读取器"""

    def __init__(self, name: str, port: int):
        """初始化传感器读取器

        Args:
            name: 组件名称
            port: socket端口号
        """
        super().__init__(name)
        self.reader = self._setup_socket_reader(port)

    def setup_instance(self, instance: int) -> None:
        """设置实例"""
        pass

    def get_events(self, collector: List[Event]) -> None:
        """从socket读取车辆类型并生成事件

        Args:
            collector: 事件收集器
        """
        try:
            # 读取一行数据作为vehicle type
            vehicle_type = self.reader.readline().strip()
            if not vehicle_type:
                # 如果服务器关闭，退出程序
                sys.exit(0)

            # 创建事件并收集
            collector.append(VehicleEvent(vehicle_type))

            # 打印日志
            print("")  # 在新事件之前打印空行
            print(f"SensorReader --> {vehicle_type}")

        except Exception as e:
            print(f"Failed to read input: {e}")

    def _setup_socket_reader(self, port: int):
        """设置socket连接

        Args:
            port: 端口号

        Returns:
            socket读取器
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("localhost", port))
            return sock.makefile("r")
        except Exception as e:
            print(f"Failed to setup socket: {e}")
            sys.exit(0)


class VehicleEvent(Event):
    """车辆事件"""

    def __init__(self, vehicle_type: str):
        """初始化车辆事件

        Args:
            vehicle_type: 车辆类型
        """
        self.vehicle_type = vehicle_type

    def get_data(self) -> str:
        """获取事件数据

        Returns:
            车辆类型
        """
        return self.vehicle_type

from typing import List
import socket
from ..core.source import Source
from ..core.event import Event
from .vehicle_event import VehicleEvent


class SensorReader(Source):
    """车辆传感器读取器"""

    def __init__(self, name: str, port: int):
        """初始化传感器读取器

        Args:
            name: 组件名称
            port: 监听端口
        """
        super().__init__(name)
        self.reader = self._setup_socket_reader(port)

    def setup_instance(self, instance: int) -> None:
        """设置实例"""
        pass

    def get_events(self, collector: List[Event]) -> None:
        """从socket读取车辆事件

        Args:
            collector: 事件收集器
        """
        try:
            # 读取一行数据
            vehicle = self.reader.readline().decode("utf-8").strip()
            if not vehicle:
                # 如果服务器关闭，则退出
                print("Server closed connection")
                exit(0)

            # 创建事件并收集
            event = VehicleEvent(vehicle)
            collector.append(event)

            # 打印日志
            print("")  # 在新事件之前打印空行
            print(f"SensorReader --> {vehicle}")

        except Exception as e:
            print(f"Failed to read input: {e}")

    def _setup_socket_reader(self, port: int) -> socket.io.BufferedRWPair:
        """设置socket连接

        Args:
            port: 连接端口

        Returns:
            socket读取器
        """
        try:
            # 创建socket连接
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("localhost", port))
            return sock.makefile("rwb")

        except Exception as e:
            print(f"Failed to setup socket: {e}")
            exit(0)

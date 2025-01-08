from typing import List
import socket
from ..core.source import Source
from ..core.event import Event
from .vehicle_event import VehicleEvent


class SensorReader(Source):
    """车辆传感器读取器"""

    def __init__(self, name: str, port: int, parallelism: int = 1):
        """初始化传感器读取器

        Args:
            name: 组件名称
            port: 基础端口号
            parallelism: 并行度
        """
        super().__init__(name, parallelism)
        self.base_port = port
        self.reader = None
        self.instance_id = None

    def setup_instance(self, instance: int) -> None:
        """设置实例

        Args:
            instance: 实例ID
        """
        self.instance_id = instance
        # 每个实例使用不同的端口
        port = self.base_port + instance
        self.reader = self._setup_socket_reader(port)
        print(f"Sensor reader instance {instance} listening on port {port}")

    def get_events(self, collector: List[Event]) -> None:
        """从socket读取车辆事件

        Args:
            collector: 事件收集器
        """
        try:
            # 读取一行数据
            vehicle = self.reader.readline().decode("utf-8").strip()
            if not vehicle:
                print("Server closed connection")
                exit(0)

            # 创建事件并收集
            event = VehicleEvent(vehicle)
            collector.append(event)

            # 打印日志
            print("")  # 在新事件之前打印空行
            print(f"SensorReader[{self.instance_id}] --> {vehicle}")

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
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("localhost", port))
            return sock.makefile("rwb")

        except Exception as e:
            print(f"Failed to setup socket: {e}")
            exit(0)

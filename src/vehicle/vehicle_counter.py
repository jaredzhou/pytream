from typing import List, Dict
from ..core.operator import Operator
from ..core.event import Event
from ..core.grouping_strategy import FieldGrouping


class VehicleCounter(Operator):
    """车辆计数器"""

    def __init__(self, name: str, parallelism: int = 1):
        """初始化计数器

        Args:
            name: 组件名称
            parallelism: 并行度
        """
        super().__init__(name, parallelism)
        self.counts: Dict[str, int] = {}
        # 使用车辆类型作为分组字段
        self.set_grouping_strategy(FieldGrouping("vehicle_type"))

    def setup_instance(self, instance: int) -> None:
        """设置实例

        Args:
            instance: 实例ID
        """
        self.instance_id = instance
        print(f"Vehicle counter instance {instance} initialized")

    def apply(self, event: Event, collector: List[Event]) -> None:
        """处理车辆事件

        Args:
            event: 输入事件
            collector: 输出事件收集器
        """
        # 获取车辆类型
        vehicle_type = event.get_data()

        # 更新计数
        if vehicle_type not in self.counts:
            self.counts[vehicle_type] = 0
        self.counts[vehicle_type] += 1

        # 打印所有类型的统计
        print(f"\nCounter[{self.instance_id}] 当前统计:")
        for type_, count in sorted(self.counts.items()):
            print(f"  {type_}: {count}")

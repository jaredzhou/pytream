from typing import Dict, List, Optional, override
from .core import Operator, Stream
from .event import Event, TimedEvent
from .grouping_strategy import GroupingStrategy
import time


class EventWindow:
    """事件窗口类"""

    def __init__(self, start_time: int, end_time: int):
        self.events: List[Event] = []
        self.start_time = start_time  # 窗口开始时间(包含)
        self.end_time = end_time  # 窗口结束时间(不包含)

    def add(self, event: Event) -> None:
        """添加事件到窗口"""
        self.events.append(event)

    def get_events(self) -> List[Event]:
        """获取窗口中的所有事件"""
        return self.events

    def get_start_time(self) -> int:
        """获取窗口开始时间"""
        return self.start_time

    def get_end_time(self) -> int:
        """获取窗口结束时间"""
        return self.end_time


class WindowingStrategy:
    """窗口策略接口"""

    def add(self, event: Event, processing_time: int) -> None:
        """添加事件到窗口策略"""
        pass

    def get_event_windows(self, processing_time: int) -> List[EventWindow]:
        """获取准备好的事件窗口"""
        pass


class SlidingTimeWindowingStrategy(WindowingStrategy):
    """滑动时间窗口策略"""

    def __init__(self, length_millis: int, interval_millis: int, watermark_millis: int):
        self.length_millis = length_millis  # 窗口长度
        self.interval_millis = interval_millis  # 滑动间隔
        self.watermark_millis = watermark_millis  # 水位线延迟
        self.event_windows: Dict[int, EventWindow] = {}  # 窗口集合

    @override
    def add(self, event: Event, processing_time: int) -> None:
        """添加事件到窗口策略"""
        if not isinstance(event, TimedEvent):
            raise RuntimeError(
                "Timed events are required by time based WindowingStrategy"
            )
        timed_event = event
        event_time = timed_event.get_time()
        if not self.is_late_event(event_time, processing_time):
            # 添加事件到所有覆盖其时间的窗口
            most_recent_start_time = (
                event_time // self.interval_millis * self.interval_millis
            )
            start_time = most_recent_start_time
            # 迭代直到窗口关闭时间早于事件时间
            while event_time < start_time + self.length_millis:
                window = self.event_windows.get(start_time)
                if window is None:
                    window = EventWindow(start_time, start_time + self.length_millis)
                    self.event_windows[start_time] = window
                window.add(event)
                start_time -= self.interval_millis

    def is_late_event(self, event_time: int, processing_time: int) -> bool:
        """判断事件是否为迟到事件"""
        return event_time + self.watermark_millis < processing_time

    def get_event_windows(self, processing_time: int) -> List[EventWindow]:
        """获取准备好的事件窗口"""
        result = []
        # 创建要删除的窗口起始时间列表的副本
        expired_starts = []

        # 使用字典的副本进行遍历
        for start_time, window in list(self.event_windows.items()):
            if processing_time >= window.end_time + self.watermark_millis:
                result.append(window)
                expired_starts.append(start_time)

        # 在遍历完成后再删除过期窗口
        for start_time in expired_starts:
            self.event_windows.pop(start_time, None)

        return result


class FixedTimeWindowingStrategy(SlidingTimeWindowingStrategy):
    """固定时间窗口策略"""

    def __init__(self, length_millis: int, watermark_millis: int):
        super().__init__(length_millis, length_millis, watermark_millis)


class WindowOperator(Operator):
    """窗口算子基类"""

    def __init__(
        self, name: str, parallelism: int, grouping: Optional[GroupingStrategy] = None
    ):
        super().__init__(name, parallelism, grouping)

    def apply(self, event: Event, collector: List[Event]) -> None:
        """窗口算子不支持直接处理事件"""
        raise RuntimeError(
            "apply(Event, List[Event]) is not supported by WindowOperator"
        )

    def apply_window(self, window: EventWindow, collector: List[Event]) -> None:
        """处理窗口的抽象方法"""
        pass


class WindowingOperator(Operator):
    """内部使用的窗口算子实现类"""

    def __init__(
        self,
        name: str,
        parallelism: int,
        strategy: WindowingStrategy,
        operator: WindowOperator,
        grouping: Optional[GroupingStrategy] = None,
    ):
        """初始化窗口算子

        Args:
            name: 算子名称
            parallelism: 并行度
            strategy: 窗口策略
            operator: 用户定义的窗口算子
            grouping: 分组策略
        """
        super().__init__(name, parallelism, grouping)
        self.strategy = strategy
        self.operator = operator

    def setup_instance(self, instance: int) -> None:
        """设置算子实例"""
        self.operator.setup_instance(instance)

    def apply(self, event: Event, collector: List[Event]) -> None:
        """处理输入事件

        1. 将事件添加到窗口策略
        2. 检查是否有窗口需要触发
        3. 对触发的窗口执行计算
        """
        processing_time = int(time.time() * 1000)

        # 添加事件到窗口
        if event is not None:
            self.strategy.add(event, processing_time)

        # 获取并处理准备好的窗口
        windows = self.strategy.get_event_windows(processing_time)
        for window in windows:
            self.operator.apply_window(window, collector)

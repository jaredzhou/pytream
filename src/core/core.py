from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, override, Union, TYPE_CHECKING
from .event import Event, NamedEvent, EventCollector
from .grouping_strategy import GroupingStrategy, RoundRobinGrouping, AllGrouping

if TYPE_CHECKING:
    from .window import WindowingStrategy, WindowOperator


class Component(ABC):
    """所有组件(Source和Operator)的基类"""

    def __init__(self, name: str, parallelism: int = 1):
        """初始化组件

        Args:
            name: 组件名称
            parallelism: 并行度
        """
        self.name = name
        self.parallelism = parallelism
        self._outgoing_stream: Optional[Stream] = None

    def get_name(self) -> str:
        """获取组件名称"""
        return self.name

    def get_parallelism(self) -> int:
        """获取组件并行度"""
        return self.parallelism

    def get_outgoing_stream(self) -> "Stream":
        """获取组件的输出流"""
        if self._outgoing_stream is None:
            self._outgoing_stream = Stream()
        return self._outgoing_stream


class Source(Component, ABC):
    """数据源基类"""

    def __init__(self, name: str, parallelism: int = 1):
        """初始化数据源

        Args:
            name: 数据源名称
            parallelism: 并行度
        """
        super().__init__(name, parallelism)

    @abstractmethod
    def setup_instance(self, instance: int) -> None:
        """设置数据源实例

        Args:
            instance: 实例ID(从0开始的索引)
        """
        pass

    @abstractmethod
    def get_events(self, collector: List[Event]) -> None:
        """获取事件

        Args:
            collector: 事件收集器
        """
        pass


class Operator(Component, ABC):
    """算子基类,用于处理流数据"""

    def __init__(
        self,
        name: str,
        parallelism: int = 1,
        grouping: Union[
            GroupingStrategy, Dict[str, GroupingStrategy]
        ] = RoundRobinGrouping(),
    ):
        """初始化算子

        Args:
            name: 算子名称
            parallelism: 并行度
            grouping: 默认分组策略
        """
        super().__init__(name, parallelism)
        # 为每个流存储不同的分组策略
        if isinstance(grouping, GroupingStrategy):
            self._grouping_map: Dict[str, GroupingStrategy] = {"default": grouping}
        elif isinstance(grouping, Dict):
            self._grouping_map = grouping

    def get_grouping_strategy(self, stream_name: str = "default") -> GroupingStrategy:
        """获取指定流的分组策略

        Args:
            stream_name: 流名称，默认为"default"

        Returns:
            对应流的分组策略
        """
        print(f"stream_name: {stream_name} gmap: {self._grouping_map}")
        return self._grouping_map.get(
            stream_name,
        )

    def get_grouping_strategy_map(self) -> Dict[str, GroupingStrategy]:
        """获取所有流的分组策略映射

        Returns:
            分组策略映射字典
        """
        return self._grouping_map

    @abstractmethod
    def setup_instance(self, instance: int) -> None:
        """设置算子实例

        Args:
            instance: 实例ID(从0开始的索引)
        """
        pass

    @abstractmethod
    def apply(self, stream_name: str, event: Event, collector: List[Event]) -> None:
        """处理输入事件并生成结果

        Args:
            stream_name: 输入流名称
            event: 输入事件
            collector: 输出事件收集器
        """
        pass

    def process(self, event: Event, collector: List[Event]) -> None:
        """处理事件的入口方法

        如果是NamedEvent，则使用其流名称；否则使用默认流名称

        Args:
            event: 输入事件
            collector: 输出事件收集器
        """
        stream_name = "default"
        if isinstance(event, NamedEvent):
            stream_name = event.get_stream_name()
        self.apply(stream_name, event, collector)


class JoinOperator(Operator):
    """JoinOperator"""

    DEFAULT_GROUPING = AllGrouping()

    def __init__(
        self, name: str, parallelism: int, grouping_map: Dict[str, GroupingStrategy]
    ):
        super().__init__(name, parallelism, grouping_map)

    @override
    def get_grouping_strategy(self, stream_name: str = "default") -> GroupingStrategy:
        strategy = super().get_grouping_strategy(stream_name)
        if strategy is None:
            return self.DEFAULT_GROUPING


class StreamChannel:
    """表示流的特定通道"""

    def __init__(self, base_stream: "Stream", channel: str):
        self.base_stream = base_stream
        self.channel = channel

    @override
    def apply_operator(self, operator: "Operator") -> "Stream":
        """将算子应用到指定通道"""
        return self.base_stream.apply_operator(operator, self.channel)


class Stream:
    """数据流类,代表从组件输出的数据流"""

    DEFAULT_CHANNEL = "default"
    DEFAULT_STREAM = "default"

    def __init__(self):
        # 每个通道对应一组下游算子
        self._downstream_operators: Dict[str, Dict[str, "Operator"]] = {}

    def apply_operator(
        self,
        operator: "Operator",
        channel: str = DEFAULT_CHANNEL,
        stream_name: str = DEFAULT_STREAM,
    ) -> "Stream":
        """将算子应用到此数据流的指定通道"""
        if channel not in self._downstream_operators:
            self._downstream_operators[channel] = {}

        operator_map = self._downstream_operators[channel]
        if operator in operator_map:
            raise RuntimeError(f"Operator {operator.get_name()} is added to job twice")

        operator_map[stream_name] = operator
        return operator.get_outgoing_stream()

    def select_channel(self, channel: str) -> "StreamChannel":
        """选择指定的通道"""

        return StreamChannel(self, channel)

    def get_channels(self) -> Set[str]:
        """获取所有通道名称"""
        return set(self._downstream_operators.keys())

    def get_applied_operators(self, channel: str) -> Dict[str, "Operator"]:
        """获取指定通道的下游算子集合"""
        return self._downstream_operators.get(channel, {})

    def with_windowing(
        self, strategy: Union["WindowingStrategy", Dict[str, "WindowingStrategy"]]
    ) -> "WindowedStream":
        """添加窗口策略到流"""
        from ..core.window import WindowingStrategy

        if isinstance(strategy, WindowingStrategy):
            return WindowedStream(self, {self.DEFAULT_STREAM: strategy})
        elif isinstance(strategy, Dict):
            return WindowedStream(self, strategy)


class WindowedStream(Stream):
    """支持窗口操作的流"""

    def __init__(self, base_stream: Stream, strategy: WindowingStrategy):
        self.base_stream = base_stream
        self.strategy = strategy

    def apply_operator(self, operator: "WindowOperator") -> "Stream":
        """应用窗口算子到流"""
        from .window import WindowingOperator

        windowing_operator = WindowingOperator(
            name=operator.get_name(),
            parallelism=operator.get_parallelism(),
            strategy=self.strategy,
            operator=operator,
            grouping=operator.get_grouping_strategy(),
        )
        return self.base_stream.apply_operator(windowing_operator)


class Streams:
    """多个数据流的集合，用于合并多个流"""

    def __init__(self, streams: List[Stream]):
        self.streams = streams

    @staticmethod
    def of(*streams: Stream) -> "Streams":
        """创建Streams实例

        Example:
            Streams.of(stream1, stream2, stream3).apply_operator(operator)
            或
            Streams.of(stream1.select_channel("second_channel"), stream2).apply_operator(operator)
        """
        return Streams(list(streams))


class NamedStreams:
    """命名流集合，用于管理多个命名的输入流"""

    def __init__(self, named_streams: Dict[str, Stream]):
        """初始化命名流集合

        Args:
            named_streams: 流名称到Stream对象的映射
        """
        self._named_streams = named_streams

    @classmethod
    def of(cls, named_streams: Dict[str, Stream]) -> "NamedStreams":
        """创建NamedStreams实例

        Args:
            named_streams: 流名称到Stream对象的映射

        Returns:
            NamedStreams实例
        """
        return cls(named_streams)

    def join(self, operator: Operator) -> Stream:
        """对流集合应用join算子

        Args:
            operator: join算子

        Returns:
            join后的输出流
        """
        # 为每个命名流应用算子
        for stream_name, stream in self._named_streams.items():
            print(f"stream_name: {stream_name}, stream: {stream}")
            # 获取该流的分组策略
            grouping = operator.get_grouping_strategy(stream_name)
            # 将算子应用到流上，并传入流名称
            stream.apply_operator(operator, stream_name=stream_name)

        # 返回算子的输出流
        return operator.get_outgoing_stream()

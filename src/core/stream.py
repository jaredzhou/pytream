from typing import Dict, List, Set, TYPE_CHECKING, override

if TYPE_CHECKING:
    from .operator import Operator


class Stream:
    """数据流类,代表从组件输出的数据流"""

    DEFAULT_CHANNEL = "default"

    def __init__(self):
        # 每个通道对应一组下游算子
        self._downstream_operators: Dict[str, Set["Operator"]] = {}

    def apply_operator(
        self, operator: "Operator", channel: str = DEFAULT_CHANNEL
    ) -> "Stream":
        """将算子应用到此数据流的指定通道"""
        if channel not in self._downstream_operators:
            self._downstream_operators[channel] = set()

        operator_set = self._downstream_operators[channel]
        if operator in operator_set:
            raise RuntimeError(f"Operator {operator.get_name()} is added to job twice")

        operator_set.add(operator)
        return operator.get_outgoing_stream()

    def select_channel(self, channel: str) -> "StreamChannel":
        """选择指定的通道"""
        return StreamChannel(self, channel)

    def get_channels(self) -> Set[str]:
        """获取所有通道名称"""
        return set(self._downstream_operators.keys())

    def get_applied_operators(self, channel: str) -> Set["Operator"]:
        """获取指定通道的下游算子集合"""
        return self._downstream_operators.get(channel, set())


class StreamChannel(Stream):
    """表示流的特定通道"""

    def __init__(self, base_stream: Stream, channel: str):
        self.base_stream = base_stream
        self.channel = channel

    @override
    def apply_operator(self, operator: "Operator") -> "Stream":
        """将算子应用到指定通道"""
        return self.base_stream.apply_operator(operator, self.channel)


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

    @override
    def apply_operator(self, operator: "Operator") -> Stream:
        """将算子应用到所有流"""
        for stream in self.streams:
            stream.apply_operator(operator)
        return operator.get_outgoing_stream()

from typing import Dict
from .stream import Stream
from .operator import Operator
from .grouping_strategy import GroupingStrategy


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
            # 获取该流的分组策略
            grouping = operator.get_grouping_strategy(stream_name)
            # 将算子应用到流上，并传入流名称
            stream.apply_operator(operator, stream_name)

        # 返回算子的输出流
        return operator.get_outgoing_stream()

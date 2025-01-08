from abc import ABC
from typing import Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from .operator import Operator
from .stream import Stream


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

    def get_outgoing_stream(self) -> Stream:
        """获取组件的输出流"""
        if self._outgoing_stream is None:
            self._outgoing_stream = Stream()
        return self._outgoing_stream

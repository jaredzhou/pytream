from typing import Set
from .source import Source
from .stream import Stream

class Job:
    """作业类,用于设置和运行流处理作业"""
    
    def __init__(self, name: str):
        """初始化作业
        
        Args:
            name: 作业名称
        """
        self.name = name
        self._source_set: Set[Source] = set()
        
    def add_source(self, source: Source) -> Stream:
        """添加数据源到作业
        
        Args:
            source: 要添加到作业的数据源对象
            
        Returns:
            可用于连接其他算子的流
            
        Raises:
            RuntimeError: 如果数据源被添加两次
        """
        if source in self._source_set:
            raise RuntimeError(f"Source {source.get_name()} is added to job twice")
            
        self._source_set.add(source)
        return source.get_outgoing_stream()
        
    def get_name(self) -> str:
        """获取作业名称"""
        return self.name
        
    def get_sources(self) -> Set[Source]:
        """获取所有数据源"""
        return self._source_set

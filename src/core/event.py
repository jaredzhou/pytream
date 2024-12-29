from abc import ABC, abstractmethod
from typing import Any, Dict

class Event(ABC):
    """事件基类,代表流中传递的数据单元"""
    
    def __init__(self, data: Dict[str, Any]):
        """初始化事件
        
        Args:
            data: 事件数据字典
        """
        self._data = data
        
    def get_field(self, field: str) -> Any:
        """获取事件字段值
        
        Args:
            field: 字段名
            
        Returns:
            字段值
        """
        return self._data.get(field)
        
    def get_fields(self) -> Dict[str, Any]:
        """获取所有字段
        
        Returns:
            事件数据字典
        """
        return self._data
        
    @abstractmethod
    def get_data(self) -> Any:
        """获取事件的数据值
        
        Returns:
            事件数据
        """
        pass

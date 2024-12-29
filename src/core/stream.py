from typing import Set, Optional

class Stream:
    """数据流类,代表从组��输出的数据流"""
    
    DEFAULT_CHANNEL = "default"
    
    def __init__(self):
        self._operator_set: Set['Operator'] = set()
        
    def apply_operator(self, operator: 'Operator', channel: str = DEFAULT_CHANNEL) -> 'Stream':
        """将算子应用到此数据流
        
        Args:
            operator: 要连接到当前流的算子
            channel: 数据通道名称
            
        Returns:
            算子的输出流
        """
        if operator in self._operator_set:
            raise RuntimeError(f"Operator {operator.get_name()} is added to job twice")
            
        self._operator_set.add(operator)
        return operator.get_outgoing_stream()

from typing import List
from .process import Process
from ..event import Event
from .event_queue import EventQueue

class ComponentExecutor(Process):
    """组件执行器基类"""
    
    def __init__(self, component):
        super().__init__()
        self.component = component
        self.event_collector: List[Event] = []
        self.incoming_queue: EventQueue = None
        self.outgoing_queue: EventQueue = None
        
    def set_incoming_queue(self, queue: EventQueue):
        self.incoming_queue = queue
        
    def set_outgoing_queue(self, queue: EventQueue):
        self.outgoing_queue = queue

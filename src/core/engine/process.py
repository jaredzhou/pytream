from abc import ABC, abstractmethod
import threading

class Process(ABC):
    """进程基类,用于执行组件"""
    
    def __init__(self):
        self.thread = None
        self.running = False
        
    def start(self):
        """启动进程"""
        self.running = True
        self.thread = threading.Thread(target=self._run)
        self.thread.start()
        
    def stop(self):
        """停止进程"""
        self.running = False
        if self.thread:
            self.thread.join()
            
    def _run(self):
        """运行循环"""
        while self.running:
            if not self.run_once():
                break
                
    @abstractmethod
    def run_once(self) -> bool:
        """执行一次处理
        
        Returns:
            是否继续执行
        """
        pass

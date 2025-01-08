.PHONY: vehicle clean

# Python 解释器
PYTHON = python3

# 源代码目录
SRC_DIR = src

# 运行vehicle示例
vehicle:
	PYTHONPATH=./ $(PYTHON) -m src.vehicle.main

fraud:
	PYTHONPATH=./ $(PYTHON) -m src.fraud.fraud
	
# 清理生成的文件
clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete 
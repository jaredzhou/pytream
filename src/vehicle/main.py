import argparse
from ..core.job import Job
from ..core.engine.stream_engine import StreamEngine
from ..core.grouping_strategy import RoundRobinGrouping, FieldGrouping
from .sensor_reader import SensorReader
from .vehicle_counter import VehicleCounter


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="Vehicle Monitoring System")

    # 添加并行度参数
    parser.add_argument(
        "-p", "--parallelism", type=int, default=1, help="并行度 (默认: 1)"
    )

    # 添加分组策略参数
    parser.add_argument(
        "-g",
        "--grouping",
        choices=["round_robin", "field"],
        default="round_robin",
        help="分组策略: round_robin (轮询) 或 field (按车型分组) (默认: round_robin)",
    )

    # 添加基础端口参数
    parser.add_argument(
        "--port", type=int, default=9990, help="传感器基础端口号 (默认: 9990)"
    )

    return parser.parse_args()


def main():
    # 解析命令行参数
    args = parse_args()

    # 创建作业
    job = Job("vehicle_monitoring")

    # 创建数据源 (使用2个传感器实例)
    sensor = SensorReader("vehicle_sensor", args.port, parallelism=2)

    # 创建计数器并设置分组策略
    counter = VehicleCounter("vehicle_counter", parallelism=args.parallelism)
    if args.grouping == "round_robin":
        counter.set_grouping_strategy(RoundRobinGrouping())
        print(f"使用轮询分组策略，{args.parallelism}个计数器实例")
    else:  # field
        counter.set_grouping_strategy(FieldGrouping("vehicle_type"))
        print(f"使用车型分组策略，{args.parallelism}个计数器实例")

    # 构建处理流程
    job.add_source(sensor).apply_operator(counter)

    # 创建引擎并提交作业
    print(f"开始监控车辆，传感器端口: {args.port}-{args.port+1}")
    engine = StreamEngine()
    engine.submit(job)

    # 等待用户输入来停止
    input("按回车键停止...")


if __name__ == "__main__":
    main()

from ..core.job import Job
from .sensor_reader import SensorReader
from .vehicle_counter import VehicleCounter


def main():
    # 创建作业
    job = Job("vehicle_monitoring")

    # 创建数据源和算子
    sensor = SensorReader("vehicle_sensor", 9990)
    counter = VehicleCounter("vehicle_counter")

    # 构建处理流程
    job.add_source(sensor).apply_operator(counter)

    # 模拟运行
    sources = job.get_sources()
    for source in sources:
        events = []
        while True:
            source.get_events(events)
            if not events:
                break

            for event in events:
                counter.apply(event, [])
            events.clear()

    print("Job completed!")


if __name__ == "__main__":
    main()

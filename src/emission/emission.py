from ..core.job import Job
from ..core.core import Source, Operator, Streams, NamedStreams
from ..core.event import Event, EventCollector, TimedEvent
from typing import List, override, cast, Dict
from ..core.grouping_strategy import (
    FieldGrouping,
    GroupingStrategy,
    AllGrouping,
    RoundRobinGrouping,
)
import socket
import time
from ..core.engine import StreamEngine
from ..core.window import FixedTimeWindowingStrategy


class VehicleEvent(Event):
    def __init__(self, make: str, model: str, year: int, zone: int, timestamp: int):
        super().__init__({})
        self.make = make
        self.model = model
        self.year = year
        self.zone = zone
        self.timestamp = timestamp

    @override
    def __str__(self) -> str:
        return f"VehicleEvent(make={self.make}, model={self.model}, year={self.year}, zone={self.zone}, timestamp={self.timestamp})"


class VehicleEventSource(Source):
    def __init__(self, name: str, parallelism: int, port: int):
        super().__init__(name, parallelism)
        self.port = port

    @override
    def setup_instance(self, instance: int) -> None:
        self.port = self.port + instance
        self.reader = self.setup_socket_reader(self.port)

    @override
    def get_events(self, event_collector: EventCollector) -> None:
        try:
            vehicle = self.reader.readline().decode("utf-8").strip()
            if not vehicle:
                return
            try:
                make, model, year, zone = vehicle.split(",")
                event = VehicleEvent(make, model, year, zone, time.time() * 1000)
                event_collector.add(event)
            except Exception as e:
                print(f"Invalid vehicle data: {e}{vehicle}")
        except Exception as e:
            print(f"Error reading vehicle data: {e}")

    def setup_socket_reader(self, port: int) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        return sock.makefile("rwb")


class TemperatureEvent(Event):
    def __init__(self, temperature: float, zone: int, timestamp: int):
        super().__init__({})
        self.temperature = temperature
        self.zone = zone
        self.timestamp = timestamp

    @override
    def __str__(self) -> str:
        return f"TemperatureEvent(temperature={self.temperature}, zone={self.zone}, timestamp={self.timestamp})"


class TemperatureEventSource(Source):
    def __init__(self, name: str, parallelism: int, port: int):
        super().__init__(name, parallelism)
        self.port = port

    @override
    def setup_instance(self, instance: int) -> None:
        self.port = self.port + instance
        self.reader = self.setup_socket_reader(self.port)

    @override
    def get_events(self, event_collector: EventCollector) -> None:
        try:
            temperature = self.reader.readline().decode("utf-8").strip()
            if not temperature:
                return
            temperature, zone = temperature.split(",")
            event = TemperatureEvent(temperature, zone, time.time() * 1000)
            event_collector.add(event)
        except Exception as e:
            print(f"Error reading temperature data: {e}")

    def setup_socket_reader(self, port: int) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        return sock.makefile("rwb")


class VehicleTemperatureEvent(Event):
    def __init__(
        self,
        make: str,
        model: str,
        year: int,
        zone: int,
        temperature: float,
        timestamp: int,
    ):
        super().__init__()
        self.make = make
        self.model = model
        self.year = year
        self.zone = zone
        self.temperature = temperature
        self.timestamp = timestamp

    @override
    def __str__(self) -> str:
        return f"VehicleTemperatureEvent(make={self.make}, model={self.model}, year={self.year}, zone={self.zone}, temperature={self.temperature}, timestamp={self.timestamp})"


class EmissionEvent(TimedEvent):
    def __init__(self, timestamp: int, zone: int, emission: float):
        super().__init__()
        self.timestamp = timestamp
        self.zone = zone
        self.emission = emission

    @override
    def get_time(self) -> int:
        return self.timestamp

    @override
    def __str__(self) -> str:
        return f"EmissionEvent(timestamp={self.timestamp}, zone={self.zone}, emission={self.emission})"


class EmissionResolver(Operator):
    def __init__(self, name: str, parallelism: int):
        super().__init__(name, parallelism)

    @override
    def apply(
        self, stream_name: str, event: Event, event_collector: EventCollector
    ) -> None:
        vehicle_temperature_event = event
        emission = self.emission_table.get_emission(
            vehicle_temperature_event.make,
            vehicle_temperature_event.model,
            vehicle_temperature_event.year,
            vehicle_temperature_event.temperature,
            4,
        )
        emission_event = EmissionEvent(
            vehicle_temperature_event.timestamp,
            vehicle_temperature_event.zone,
            emission,
        )
        event_collector.add(emission_event)

    @override
    def setup_instance(self, instance: int) -> None:
        self.instance = instance


class ZoneFieldsGrouping(FieldGrouping):

    def get_key(self, event: Event) -> int:
        return event.zone


class WindowedAggregator(Operator):
    def __init__(self, name: str, parallelism: int, grouping: GroupingStrategy):
        super().__init__(name, parallelism, grouping)

    @override
    def setup_instance(self, instance: int) -> None:
        self.instance = instance

    @override
    def apply(
        self, stream_name: str, event: Event, event_collector: EventCollector
    ) -> None:
        emissions = {}
        for event in event_collector.get_events():
            emission = event.emission
            zone = event.zone
            if zone in emissions:
                emissions[zone] += emission
            else:
                emissions[zone] = emission
        for zone, emission in emissions.items():
            print(
                f"WindowedAggregator :: instance {self.instance} --> total emission for zone {zone} between {event_collector.get_start_time()} and {event_collector.get_end_time()} is {emission}"
            )


class EventJoiner(Operator):
    DEFAULT_TEMPERATURE = 60

    def __init__(
        self, name: str, parallelism: int, grouping: Dict[str, GroupingStrategy]
    ):
        super().__init__(name, parallelism, grouping)
        self.temperature_table = {}

    def apply(self, stream_name: str, event: Event, collector: List[Event]) -> None:
        if stream_name == "temperature":
            # 类型检查和转换
            if not isinstance(event, TemperatureEvent):
                raise TypeError(f"Expected TemperatureEvent but got {type(event)}")
            temperature_event = event  # 此时 IDE 会识别为 TemperatureEvent 类型
            # 或者显式转换
            temperature_event = cast(TemperatureEvent, event)
            # 使用转换后的事件
            self.temperature_table[temperature_event.zone] = (
                temperature_event.temperature
            )
            print(
                f"EventJoiner :: instance {self.instance} --> update temperature table: {temperature_event.zone} -> {temperature_event.temperature}."
            )

        else:
            # Vehicle events
            if not isinstance(event, VehicleEvent):
                raise TypeError(f"Expected VehicleEvent but got {type(event)}")
            vehicle_event = cast(VehicleEvent, event)
            temperature = self.temperature_table.get(vehicle_event.zone, 60)
            if temperature is not None:
                vehicle_temperature_event = VehicleTemperatureEvent(
                    vehicle_event.make,
                    vehicle_event.model,
                    vehicle_event.year,
                    vehicle_event.zone,
                    temperature,
                    vehicle_event.timestamp,
                )
                print(
                    f"EventJoiner :: instance {self.instance} --> found temperature: {temperature}. Patch and emit: {vehicle_temperature_event}."
                )
                collector.append(vehicle_temperature_event)
            else:
                vehicle_temperature_event = VehicleTemperatureEvent(
                    vehicle_event.make,
                    vehicle_event.model,
                    vehicle_event.year,
                    vehicle_event.zone,
                    self.DEFAULT_TEMPERATURE,
                    vehicle_event.timestamp,
                )
                print(
                    f"EventJoiner :: instance {self.instance} --> temperature not found. Default to {self.DEFAULT_TEMPERATURE} and emit {vehicle_temperature_event}."
                )
                collector.append(vehicle_temperature_event)

    def setup_instance(self, instance: int) -> None:
        self.instance = instance


def main():
    job = Job("emission job")
    vehicle_stream = job.add_source(VehicleEventSource("vehicle source", 1, 9990))
    temperature_stream = job.add_source(
        TemperatureEventSource("temperature source", 1, 9991)
    )
    NamedStreams.of(
        {"vehicle": vehicle_stream, "temperature": temperature_stream}
    ).join(
        EventJoiner(
            "join operator",
            2,
            {"vehicle": RoundRobinGrouping(), "temperature": AllGrouping()},
        )
    ).apply_operator(
        EmissionResolver("emission resolver", 1)
    ).with_windowing(
        FixedTimeWindowingStrategy(5000, 2000)
    ).apply_operator(
        WindowedAggregator("windowed aggregator", 2, ZoneFieldsGrouping())
    )

    engine = StreamEngine()
    engine.submit(job)


if __name__ == "__main__":
    main()

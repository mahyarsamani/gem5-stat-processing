from enum import Enum
from typing import Union
from collections import OrderedDict
from abc import ABCMeta, abstractmethod

from .string_manip import extract_numerical_substrings


class DataDigest(Enum):
    with_aggregate = 0
    without_aggregate = 1
    just_aggregate = 2


class Stat(metaclass=ABCMeta):
    def __init__(self, name: str, desc: str) -> None:
        self._name = name
        self._desc = desc
        self._container = None

    @abstractmethod
    def aggregate(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_to_container(self, value: tuple) -> None:
        raise NotImplementedError

    @abstractmethod
    def next_data_point(
        self, include: DataDigest = DataDigest.with_aggregate
    ) -> tuple:
        raise NotImplementedError

    @abstractmethod
    def post_process(self):
        raise NotImplementedError

    def __str__(self):
        return f"Stat(name: {self._name}, desc: {self._desc}, container: {self._container})"


class ScalarStat(Stat):
    def __init__(self, name: str, desc: str) -> None:
        super().__init__(name, desc)
        self._container = dict()

    def aggregate(self) -> None:
        agg = 0
        for value in self._container.values():
            agg += value
        self._container["aggregate"] = agg

    def add_to_container(self, value: tuple) -> None:
        assert len(value) == 2
        assert isinstance(value, tuple)
        assert isinstance(value[0], str)
        assert isinstance(value[1], float)

        self._container[value[0]] = value[1]

    def next_data_point(
        self, include: DataDigest = DataDigest.with_aggregate
    ) -> tuple:
        for owner, value in self._container.items():
            yield (owner, value)

    def post_process(self):
        self._container = OrderedDict(
            sorted(
                self._container.items(),
                key=lambda x: tuple(
                    [int(num) for num in extract_numerical_substrings(x[0])]
                ),
            )
        )


class Stats:
    def __init__(self):
        self._container = {}

    def find(self, owner_group: str, name: str) -> Union[Stat, None]:
        if (owner_group, name) in self._container:
            return self._container[(owner_group, name)]
        else:
            return None

    def insert(self, owner_group: str, name: str, stat: Stat) -> None:
        self._container[(owner_group, name)] = stat
        return stat

    def query(self, name: str):
        owner_groups = []
        for owner_group, stat_name in self._container:
            if stat_name == name:
                owner_groups.append(owner_group)
        return owner_groups

    def aggregate(self) -> None:
        for stat in self._container.values():
            stat.aggregate()

    def post_process(self) -> None:
        for stat in self._container.values():
            stat.post_process()

from dataclasses import dataclass


@dataclass
class EpochStats:
    epoch: int
    min_cost: float
    max_cost: float
    mean_cost: float
    std_cost: float

    def to_dict(self):
        dict_obj = self.__dict__
        return dict_obj

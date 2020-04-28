from dataclasses import dataclass


@dataclass
class JobRequest:
    id: str
    sub_time: int
    alpha: float
    data: int
    mass: int
    min_num_servers: int
    max_num_servers: int

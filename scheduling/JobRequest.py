from dataclasses import dataclass


@dataclass
class JobRequest:
    """A representation of a user input used to construct a Job object.
    """

    id: str  #: The JobRequest identifier.
    sub_time: int  #: The submission time of the job.
    alpha: float  #: The speedup factor alpha.
    data: int  #: An estimate of the data amount of the job.
    mass: int  #: An estimate of the amount of calculations to be performed.
    min_num_servers: int  #: The minimum required number of servers to run the job.
    max_num_servers: int  #: The maximum required number of servers to run the job.

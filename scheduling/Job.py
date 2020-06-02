from math import floor


class Job:
    """A representation of a Job: a task that can run on several servers in the same time.
    """

    POWER_OFF_ID = "POWER_OFF"  #: An identifier for power off jobs.

    def __init__(
        self,
        req_id: str,
        alpha: float,
        data: int,
        mass: int,
        max_server_count: int,
        servers: list,
        start_time,
    ):
        """Creates a Job objects.

        Args:
            req_id: The JobRequest identifier too which the Job belongs to.
            alpha: The speedup factor alpha.
            data: The amount of data.
            mass: The amount of calculations.
            max_server_count: The maximum number of required servers.
            servers: A list of all the servers of the cluster.
            start_time: The starting time of the job.

        """
        self.id = req_id  #: The JobRequest identifier too which the Job belongs to.
        self.alpha = alpha  #: The speedup factor alpha.
        self.data = data  #: The amount of data.
        self.mass = mass  #: The amount of calculations.
        self.max_server_count = max_server_count
        """The maximum number of required servers."""
        self.servers = servers  #: A list of all the servers of the cluster.
        self.server_count = len(servers)  #: The total number of servers.
        self.start_time = start_time  #: The starting time of the job.
        if self.mass > 0:
            self.end_time = self.start_time + self.exec_time(
                self.mass, self.server_count, self.alpha
            )  #: The ending time of the job.

    def __repr__(self):
        return f"{self.id}: mass: {self.mass}, start: {self.start_time}, end: {self.end_time}"

    @classmethod
    def from_request(cls, req: str, *args, **kwargs):
        """Creates a Job object from a JobRequest attributes.

        Args:
            req: The JobRequest id to which the Job is going to be constructed.
            *args: A variable length argument list, used to pass the list of servers\
            the job will be assigned to.
            **kwargs: A list of named arguments, used to inject the starting time \
            of the created job.

        Returns:
            Job: A Job object is returned.
        """
        return cls(
            req.id, req.alpha, req.data, req.mass, req.max_num_servers, *args, **kwargs
        )

    @classmethod
    def make_power_off(cls, servers: list, start_time, duration):
        """Creates a Power-off Job that shuts down in parallel a list of servers.

        Args:
            servers: The list of servers to turn off.
            start_time: The start time of the Power-Off Job.
            duration: The duration for which the servers need to remain off.

        Returns:
            Job: A Job characterized by a 0 mass and 0 alpha.
        """
        job = cls(Job.POWER_OFF_ID, 0, 0, 0, 0, servers, start_time)
        job.end_time = start_time + duration
        return job

    @staticmethod
    def exec_time(mass: int, server_count: int, alpha: float):
        """Estimates the makespan of a Job according to its mass and the\
         number of servers it is assigned to.

        Args:
            mass: The amount of computation to be performed during the Job.
            server_count: The number of servers the Job is assigned to.
            alpha: The speedup factor of the Job.

        Returns:
            The execution time of the Job.
        """
        return mass / server_count ** alpha

    @property
    def duration(self):
        """float: Computes the actual execution time of the Job."""
        return self.end_time - self.start_time

    def interupt(self, time):
        """Interrupts the execution of a Job.
        Args:
            time: The instant at which the Job is interrupted.
        """
        self.end_time = time

    def reconfigure(self, servers: list, time):
        """Reconfigures jobs on the specified servers list.

        Args:
            servers: The servers on which the jobs will be assigned after \
             reconfiguration.
            time: The instant at which the Job is reconfigured.

        Returns:
            Job: A Job characterized by a 0 mass and 0 alpha.
        """
        reconfig_time = self.reconfiguration_time(len(servers))
        reconfiguration = Job(self.id, 0, 0, 0, 0, servers, time)
        reconfiguration.end_time = time + reconfig_time
        return (
            reconfiguration,
            Job(
                self.id,
                self.alpha,
                self.data,
                self.remaining_mass(time),
                self.max_server_count,
                servers,
                time + reconfig_time,
            ),
        )

    def executed_mass(self, time):
        """Computes the executed mass so far.

        Args:
            time: The instant at which the exectued mass is calculated.

        Returns:
            The amount of calculations that have been performed since the beginning\
            of the execution of the job.
        """
        time = min(max(time, self.start_time), self.end_time)
        return (time - self.start_time) * self.server_count ** self.alpha

    def remaining_mass(self, time):
        """Computes the remaining mass of a Job.

        Args:
            time: The instant at which the remaining mass is calculated.

        Returns:
            The amount of calculations that have been performed since the beginning\
            of the execution of the job.
        """
        return self.mass - self.executed_mass(time)

    def remaining_time(self, time):
        """Computes the remaining time to finish a Job.

        Args:
            time: The instant at which the remaining time is calculated.

        Returns:
            The remaining time to finsih a Job.
        """
        return self.end_time - time

    def reconfiguration_time(self, new_server_count: int):
        """Computes the time for which a reconfiguration would last.
        Args:
            new_server_count: The number of servers the Job will be running after\
            the reconfiguration.

        Returns:
            The time needed to transfer data between servers.
        """
        maxi = max(self.server_count, new_server_count)
        mini = min(self.server_count, new_server_count)
        return self.data / maxi * floor(maxi / mini)

    def is_running(self, time):
        """Checks if a Job is still running.

        Args:
            time: The instant at which the check is performed.

        Returns:
            True if successful, False otherwise.
        """
        return self.start_time <= time < self.end_time

    def is_complete(self, time):
        """Checks if a Job is finished.

        Args:
            time: The instant at which the check is performed.

        Returns:
            True if successful, False otherwise.
        """
        return time >= self.end_time

    def is_reconfigurable(self):
        """Checks if a Job is reconfigurable.

        Returns:
            True if successful, False otherwise.
        """
        return self.mass != 0 and len(self.servers) != self.max_server_count

    def is_reconfiguration(self):
        """Checks whether a particular Job is a reconfiguration.

        Returns:
            True if successful, False otherwise.
        """
        return self.mass == 0 and self.id is not Job.POWER_OFF_ID

    def is_power_off(self):
        """Checks whether a particular Job is a Power-off.

        Returns:
            True if successful, False otherwise.
        """
        return self.mass == 0 and self.id is Job.POWER_OFF_ID

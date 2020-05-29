from enum import IntEnum

from .Job import Job


class Server:
    """A representation of a server that runs Jobs.
    """

    class Consumption(IntEnum):
        """A container class for the Server's energy consumption.
        """

        OFF = 10  #: The server's power consumption when off (in Watt).
        IDLE = 95  #: The server's power consumption when idle (in Watt).
        BOOT = 101  #: The server's power consumption when booting (in Watt).
        ACTIVE = 191  #: The server's power consumption when computing (in Watt).
        SHUTDOWN = 125  #: The server's power consumption when shuting down (in Watt).

        @classmethod
        def idle(cls, time):
            """Calulates the energy comsumption of an idle server.

            Args:
                time: The elapsed time for which the energy is calculated.

            Returns:
                The energy consumption in Watt-seconds.
            """
            return cls.IDLE * time

        @classmethod
        def active(cls, time):
            """Calulates the energy comsumption of an active server.

            Args:
                time: The elapsed time for which the energy is calculated.

            Returns:
                The energy consumption in Watt-seconds.
            """
            return cls.ACTIVE * time

        @classmethod
        def reboot(cls, time):
            """Calulates the energy comsumption of an rebooting server.

            The rebooting is made of a shuting down phase, a duration for which
            the server is off then it starts again.

            Args:
                time: The elapsed time for which the energy is calculated.

            Returns:
                The energy consumption in Watt-seconds.
            """
            off_duration = time - (Server.Duration.SHUTDOWN + Server.Duration.BOOT)
            return (
                Server.Duration.SHUTDOWN * cls.SHUTDOWN
                + off_duration * cls.OFF
                + Server.Duration.BOOT * cls.BOOT
            )

    class Duration(IntEnum):
        BOOT = 151  #: The duration in seconds for booting a server.
        SHUTDOWN = 6  #: The duration in seconds for shuting down a server.

    def __init__(self, index):
        """Creates a Server object.

        Args:
            index: The Server object's identifier.

        """
        self.index = index  #: The Server object's identifier.
        self.jobs = []  #: A List of Jobs assigned to the Server object.

    def __repr__(self):
        return f"Server-{self.index}: {self.jobs}"

    def add_job(self, job: Job):
        """Appends a job into the Server object's jobs list.

        Args:
            job: The Job object to be added to the Server object's jobs list.
        """
        self.jobs.append(job)

    def remove_job(self, job: Job):
        """Removes a job from the Server object's jobs list.

        Args:
            job: The Job object to be removed from the Server object' jobs list.
        """
        self.jobs.remove(job)

    def is_busy(self, time):
        """Checks whether a Server object is running any Jobs at a time t

        Args:
            time: The instant at which the status of the server is checked.

        Returns:
            True if successful, False otherwise.
        """
        return any(job.is_running(time) for job in self.jobs)

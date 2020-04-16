from enum import IntEnum


class Server:
    class Consumption(IntEnum):
        OFF = 10
        IDLE = 95
        BOOT = 101
        ACTIVE = 191
        SHUTDOWN = 125

        @classmethod
        def idle(cls, time):
            return cls.IDLE * time

        @classmethod
        def active(cls, time):
            return cls.ACTIVE * time

        @classmethod
        def reboot(cls, time):
            off_duration = time - (Server.Duration.SHUTDOWN + Server.Duration.BOOT)
            return (
                Server.Duration.SHUTDOWN * cls.SHUTDOWN
                + off_duration * cls.OFF
                + Server.Duration.BOOT * cls.BOOT
            )

    class Duration(IntEnum):
        BOOT = 151
        SHUTDOWN = 6

    def __init__(self, id):
        self.id = id
        self.jobs = []

    def __eq__(self, other):
        return self.id == other.id

    def add_job(self, job):
        self.jobs.append(job)

    def remove_job(self, job):
        self.jobs.remove(job)

    def is_busy(self, time):
        return any(job.is_running(time) for job in self.jobs)

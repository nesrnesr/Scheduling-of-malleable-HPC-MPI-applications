from math import floor


class Job:
    POWER_OFF_ID = "POWER_OFF"

    def __init__(
        self, req_id, alpha, data, mass, max_server_count, servers, start_time
    ):
        self.id = req_id
        self.alpha = alpha
        self.data = data
        self.mass = mass
        self.max_server_count = max_server_count
        self.servers = servers
        self.server_count = len(servers)
        self.start_time = start_time
        if self.mass > 0:
            self.end_time = self.start_time + self.exec_time(
                self.mass, self.server_count, self.alpha
            )

    def __repr__(self):
        return f"{self.id}: mass: {self.mass}, start: {self.start_time}, end: {self.end_time}"

    @classmethod
    def from_request(cls, req, *args, **kwargs):
        return cls(
            req.id, req.alpha, req.data, req.mass, req.max_num_servers, *args, **kwargs
        )

    @classmethod
    def make_power_off(cls, servers, start_time, duration):
        job = cls(Job.POWER_OFF_ID, 0, 0, 0, 0, servers, start_time)
        job.end_time = start_time + duration
        return job

    @staticmethod
    def exec_time(mass, server_count, alpha):
        return mass / server_count ** alpha

    @property
    def duration(self):
        return self.end_time - self.start_time

    def interupt(self, time):
        self.end_time = time

    def reconfigure(self, servers, time):
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
        time = min(max(time, self.start_time), self.end_time)
        return (time - self.start_time) * self.server_count ** self.alpha

    def remaining_mass(self, time):
        return self.mass - self.executed_mass(time)

    def remaining_time(self, time):
        return self.end_time - time

    def reconfiguration_time(self, new_server_count):
        maxi = max(self.server_count, new_server_count)
        mini = min(self.server_count, new_server_count)
        return self.data / maxi * floor(maxi / mini)

    def is_running(self, time):
        return self.start_time <= time < self.end_time

    def is_complete(self, time):
        return time >= self.end_time

    def is_reconfigurable(self):
        return self.mass != 0 and len(self.servers) != self.max_server_count

    def is_reconfiguration(self):
        return self.mass == 0 and self.id is not Job.POWER_OFF_ID

    def is_power_off(self):
        return self.mass == 0 and self.id is Job.POWER_OFF_ID

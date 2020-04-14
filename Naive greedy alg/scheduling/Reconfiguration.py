from .Task import Task


class Reconfiguration(Task):
    def __init__(self, job_id, servers, start_time, end_time):
        super(Reconfiguration, self).__init__(job_id, 0, servers, start_time, end_time)

    def __repr__(self):
        return "name: {}, reconfiguration, mass_exec: {}, #servers: {} start_time: {}, end_time: {}".format(
            self.job_id,
            self.mass_executed,
            len(self.servers),
            self.start_time,
            self.end_time,
        )

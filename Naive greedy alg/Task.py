class Task():
    def __init__(self, job_id, mass_executed, servers, start_time, end_time):
        self.job_id = job_id
        self.mass_executed = mass_executed
        self.servers = servers
        self.start_time = start_time
        self.end_time = end_time

    def __repr__(self):
        return "name: {}, mass_exec: {}, #servers: {} start_time: {}, end_time: {}".format(
            self.job_id, self.mass_executed, len(self.servers),
            self.start_time, self.end_time)

    def from_job(self, job):
        return self.job_id == job.id

class Scheduler(object):
    def __init__(self, servers):
        self.servers = servers
        self.jobs = []
        self.tasks = []
        self.job_queued = []

    def schedule(self, job):
        self.jobs.append(job)

        #schedule the first job into one task
        if len(self.tasks) == 0:
            self._schedule_task(self.servers, job, job.sub_time)
        #Schedule the jobs
        else:
            #Get the servers available at submission time
            available_servers = self._available_servers(job.sub_time)
            if (len(available_servers) < job.min_num_servers):
                self.job_queued.append(job)
                return
            #Schedule task
            else:
                self._schedule_task(available_servers, job, job.sub_time)

    # returns a list of servers not utilized at a given time
    def _available_servers(self, time):
        #Start with all servers as potential servers
        candidate_servers = [s for s in servers]
        #remove servers that are busy at the given time
        for t in self.tasks:
            if not (t.start_time < time and t.end_time > time):
                continue
            for s in t.servers:
                if s in candidate_servers:
                    candidate_servers.remove(s)
        return candidate_servers

    def _schedule_task(self, servers, job, time):
        num_servers = min(job.max_num_servers, len(servers))
        servers = sample(servers, k=num_servers)
        exec_time = self._exec_time(job.mass, job.alpha, num_servers)
        self.tasks.append(
            Task(job.id, job.mass, servers, time, time + exec_time))

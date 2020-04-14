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

    def update_schedule(self, time):
        #get the free servers
        available_servers = self._available_servers(time + 0.01)
        num_available_servers = len(available_servers)

        print('Number of available servers at update: ', num_available_servers)

        #Check if there is job queued
        if (self.job_queued):
            #Try to schedule each job
            for job in self.job_queued:
                if (num_available_servers > job.min_num_servers):
                    self._schedule_task(available_servers, job, time)
                    #remove job from queue
                    self.job_queued.remove(job)
                    #update list of servers available
                    available_servers = self._available_servers(time + 0.01)
                    num_available_servers = len(available_servers)

        #find tasks that could be reconfigured
        #List of tasks for which the possible change of the number of servers is greater than 0
        tasks_candidates = [
            t for t in self.tasks if
            self._task_possible_inc_num_ser(t, time, num_available_servers) > 0
        ]

        if (len(tasks_candidates) == 0):
            return

        def reconfigure_task(task):
            # Return the list of new servers to execute the task
            def reallocate_task_servers(task):
                extra_srv_count = self._task_possible_inc_num_ser(
                    task, time, num_available_servers)
                task_servers = [s for s in task.servers]
                for i in range(extra_srv_count):
                    task_servers.append(available_servers[i])
                return task_servers

            # Reconfigure and update the task
            # Update the task end_time and mass_executed.
            def interrupt_task(task, job):
                task.end_time = time
                exec_time = task.end_time - task.start_time
                task.mass_executed = self._mass_exec(job.alpha,
                                                     len(task.servers),
                                                     exec_time)

        #Create a new task for reconfiguration

            def make_reconfiguration(job, servers):
                reconfig_time = self._reconfig_time(job.data,
                                                    len(task.servers),
                                                    len(servers))
                reconfig_end_time = time + reconfig_time
                reconfig = Reconfiguration(job.id, servers, time,
                                           reconfig_end_time)
                self.tasks.append(reconfig)
                return reconfig

            # Create a task to finish job after reconfig
            def reschedule_interrupted(job, reconfig, servers):
                mass_left = job.mass - self._mass_executed(job, time)
                exec_time = self._exec_time(mass_left, job.alpha, len(servers))
                start_time = reconfig.end_time
                end_time = reconfig.end_time + exec_time
                mass_executed = mass_left
                self.tasks.append(
                    Task(job.id, mass_executed, servers, start_time, end_time))

            task_job = self._task_job(task)
            task_servers = reallocate_task_servers(task)
            interrupt_task(task, task_job)
            reconfig = make_reconfiguration(task_job, task_servers)
            reschedule_interrupted(task_job, reconfig, task_servers)

        task_to_reconfig = tasks_candidates[0]
        reconfigure_task(task_to_reconfig)

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

    # Returns the possible increase in the number of servers for a task
    # given that num_servers are not busy
    def _task_possible_inc_num_ser(self, task, time, num_servers):
        #job = list(filter(lambda j: (j.id == task.job_id), self.jobs))[0]
        if (task.end_time <= time):
            return 0
        job = next(j for j in self.jobs if (j.id == task.job_id))
        task_num_servers = len(task.servers)
        if (task_num_servers == job.max_num_servers):
            return 0
        elif (task_num_servers + num_servers > job.max_num_servers):
            return job.max_num_servers - task_num_servers
        else:
            return num_servers

    # Formula for communication time
    def _mass_exec(self, alpha, num_serv, exec_time):
        return exec_time * (num_serv)**alpha

    def _exec_time(self, mass, alpha, num_serv):
        return mass / (num_serv)**alpha

    #Calculates the reconfiguration time
    def _reconfig_time(self, data, init_servers, final_servers):
        if init_servers > final_servers:
            return data / init_servers * (ceil(init_servers / final_servers) -
                                          1)
        return data / final_servers * (ceil(final_servers / init_servers) - 1)

    #Finds how much mass has been executing of job
    def _mass_executed(self, job, time):
        mass_ex = 0
        for t in self.tasks:
            if t.job_id == job.id:
                mass_ex += t.mass_executed
        return mass_ex

    # Returns the makespan
    def work_duration(self):
        min_time = min([t.start_time for t in self.tasks])
        max_time = max([t.end_time for t in self.tasks])
        return max_time - min_time

    def job_ids(self):
        return [j.id for j in self.jobs]

    def server_ids(self):  # server_id_list
        return [s.id for s in self.servers]

    @property
    def server_count(self):
        return len(self.servers)

    #Returns the job that a task is executing
    def _task_job(self, task):
        return next(j for j in self.jobs if task.from_job(j))

        #TODO
    def jobs_reconfigured(self):
        return []  #array of jobs

    def num_jobs_reconfigured(self):
        return len(self.jobs_reconfigured())

    def stretch_time(self, job):
        #find the termination time from the tasks of that job and subtime, divide the difference by the mass
        term_time = -1
        for t in self.tasks:
            if t.job_id == job.id and t.end_time > term_time:
                term_time = t.end_time
        return (term_time - job.sub_time) / job.mass

    def stretch_times(self):
        #array of all the stretch times
        return [self.stretch_time(j) for j in self.jobs]

    def average_stretch_time(self):
        return mean(self.stretch_times())

    def max_stretch_time(self):
        return max(self.stretch_times())
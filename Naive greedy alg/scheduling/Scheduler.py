from math import ceil
from random import sample
from statistics import mean
from .Task import Task
from .Reconfiguration import Reconfiguration
from .PowerOff import PowerOff
from .Energy import Energy
from dataclasses import dataclass
import numpy as np


@dataclass
class SchedulerConfig:
    server_threshold: float = 0.7
    ratio_almost_finished_jobs: float = 0.8
    time_remaining_for_power_off: int = 370
    shut_down_time: int = 800
    estimated_improv_threshold: float = 0.9

    alpha_min_server_lower_range: float = 0.4
    alpha_min_server_mid_range: float = 0.6
    alpha_min_server_upper_range: float = 1

    alpha_lower: float = 0.65
    alpha_mid: float = 0.75

    stretch_time_weight: float = 1
    energy_weight: float = 1


class Scheduler(object):
    def __init__(self, servers, conf):
        self.servers = servers
        self.jobs = []
        self.tasks = []
        self.job_queued = []
        self.conf = conf

    def schedule1(self, job):
        self.jobs.append(job)
        self.job_queued.append(job)
        self.update_schedule1(job.sub_time)

    def update_schedule1(self, time):
        if self._is_ratio_available_servers_above_threshold(time):
            while self.job_queued:
                self._order_queue_by_sub_time()
                job_to_sched = self.job_queued[0]
                num_servers_to_use = self._num_server_alloc(job_to_sched, time)
                if num_servers_to_use > 0:
                    available_servers = self._available_servers(time)
                    self._schedule_task_given_num_servers(
                        num_servers_to_use, available_servers, job_to_sched, time
                    )
                    self.job_queued.remove(job_to_sched)
                else:
                    break
                if self._is_ratio_available_servers_above_threshold(time):
                    break

        if self._is_ratio_available_servers_above_threshold(time):
            # order job by remaining mass
            jobs_by_mass = self._jobs_by_mass_remaining(time)
            while jobs_by_mass:
                job_reconfig = jobs_by_mass[0]
                if self._is_job_reconfigurable(job_reconfig, time):
                    task_to_reconfig = self._current_task_of_job(job_reconfig, time)
                    self._reconfigure_task(task_to_reconfig, time)
                    jobs_by_mass.remove(job_reconfig)
                if self._is_ratio_available_servers_above_threshold(time):
                    break

        if self._num_available_servers(
            time
        ) != 0 and not self._is_ratio_available_servers_above_threshold(time):
            num_jobs_currently_executed = 0
            num_jobs_finishing_under_threshold_time = 0
            for job in self.jobs:
                job_termination_time = self._job_termination_time(job)
                if job_termination_time > time:
                    num_jobs_currently_executed += 1
                    time_remaining = job_termination_time - time
                    if time_remaining < self.conf.time_remaining_for_power_off:
                        num_jobs_finishing_under_threshold_time += 1
            if (
                num_jobs_currently_executed > 0
                and num_jobs_finishing_under_threshold_time
                / num_jobs_currently_executed
                <= self.conf.ratio_almost_finished_jobs
            ):
                available_servers = self._available_servers(time)
                self.turn_off_servers(available_servers, time)

    ############################################
    # Reconfigure a job if possible

    def schedule_simple(self, job):
        self.jobs.append(job)

        # schedule the first job into one task
        if len(self.tasks) == 0:
            self._schedule_task(self.servers, job, job.sub_time)
        # Schedule the jobs
        else:
            # Get the servers available at submission time
            available_servers = self._available_servers(job.sub_time)
            if len(available_servers) < job.min_num_servers:
                self.job_queued.append(job)
                return
            # Schedule task
            else:
                self._schedule_task(available_servers, job, job.sub_time)

    def update_schedule_simple(self, time):
        # get the free servers
        available_servers = self._available_servers(time + 0.01)
        num_available_servers = len(available_servers)

        # print('Number of available servers at update: ', num_available_servers)

        # Check if there is job queued
        if self.job_queued:
            # Try to schedule each job
            for job in self.job_queued:
                if num_available_servers > job.min_num_servers:
                    self._schedule_task(available_servers, job, time)
                    # remove job from queue
                    self.job_queued.remove(job)
                    # update list of servers available
                    available_servers = self._available_servers(time + 0.01)
                    num_available_servers = len(available_servers)

        # find tasks that could be reconfigured
        # List of tasks for which the possible change of the number of servers is greater than 0
        tasks_candidates = [
            t
            for t in self.tasks
            if self._task_possible_inc_num_ser(t, time, num_available_servers) > 0
        ]

        if len(tasks_candidates) == 0:
            return

        task_to_reconfig = tasks_candidates[0]
        self._reconfigure_task(task_to_reconfig, time)

    def _reconfigure_task(self, task, time):
        available_servers = self._available_servers(time + 0.01)
        num_available_servers = len(available_servers)

        # Return the list of new servers to execute the task
        def reallocate_task_servers(task):
            extra_srv_count = self._task_possible_inc_num_ser(
                task, time, num_available_servers
            )
            task_servers = [s for s in task.servers]
            for i in range(extra_srv_count):
                task_servers.append(available_servers[i])
            return task_servers

        # Reconfigure and update the task
        # Update the task end_time and mass_executed.
        def interrupt_task(task, job):
            task.end_time = time
            exec_time = task.end_time - task.start_time
            task.mass_executed = self._mass_exec(
                job.alpha, len(task.servers), exec_time
            )

        # Create a new task for reconfiguration

        def make_reconfiguration(job, servers):
            reconfig_time = self._reconfig_time(
                job.data, len(task.servers), len(servers)
            )
            reconfig_end_time = time + reconfig_time
            reconfig = Reconfiguration(job.id, servers, time, reconfig_end_time)
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
                Task(job.id, mass_executed, servers, start_time, end_time)
            )

        task_job = self._task_job(task)
        task_servers = reallocate_task_servers(task)
        interrupt_task(task, task_job)
        reconfig = make_reconfiguration(task_job, task_servers)
        reschedule_interrupted(task_job, reconfig, task_servers)

    def turn_off_servers(self, servers, time):
        self.tasks.append(PowerOff(servers, time, time + self.conf.shut_down_time))

    #################################################################
    def _is_job_reconfigurable(self, job, time):
        # print(job.id)
        if abs(job.sub_time - time) < 0.001:
            return False
        remaining_time = self._job_termination_time(job) - time
        # task to reconfigure
        task = self._current_task_of_job(job, time)
        if task is None or type(task) is Reconfiguration:
            return False
        # estimate exec time if task is reconfigured
        current_srv_count = len(task.servers)
        extra_srv_count = self._task_possible_inc_num_ser(
            task, time, self._num_available_servers(time)
        )
        new_srv_count = current_srv_count + extra_srv_count
        # Reconfiguration time
        reconfig_time = self._reconfig_time(job.data, current_srv_count, new_srv_count)

        # Mass remaining to execute until time
        mass_left = task.mass_executed - self._mass_exec(
            job.alpha, len(task.servers), time - task.start_time
        )

        # execution time on new srv count
        exec_time = self._exec_time(mass_left, job.alpha, new_srv_count)
        if (
            reconfig_time + exec_time
        ) / remaining_time < self.conf.estimated_improv_threshold:
            return True
        else:
            return False

    def _current_task_of_job(self, job, time):
        for t in self.tasks:
            if time > t.start_time and time < t.end_time and t.job_id == job.id:
                return t

    def _job_termination_time(self, job):
        term_time = -1
        for t in self.tasks:
            if t.job_id == job.id and t.end_time > term_time:
                term_time = t.end_time
        return term_time

    def _job_start_time(self, job):
        start_time = inf
        for t in self.tasks:
            if t.job_id == job.id and t.start_time < start_time:
                start_time = t.start_time
        return start_time

    ######## Can be probably tuned as well #################
    def _num_server_alloc(self, job, time):
        num_available_servers = self._num_available_servers(time)

        if job.alpha > self.conf.alpha_mid:
            min_servers = ceil(
                self.conf.alpha_min_server_upper_range * len(self.servers)
            )
            return min(min_servers, job.max_num_servers, num_available_servers)
        elif job.alpha > self.conf.alpha_lower:
            min_servers = ceil(self.conf.alpha_min_server_mid_range * len(self.servers))
            return min(min_servers, job.max_num_servers, num_available_servers)
        else:
            min_servers = ceil(
                self.conf.alpha_min_server_lower_range * len(self.servers)
            )
            return min(min_servers, job.max_num_servers, num_available_servers)

    def _order_queue_by_sub_time(self):
        self.job_queued = sorted(self.job_queued, key=lambda k: k.sub_time)

    def _is_ratio_available_servers_above_threshold(self, time):
        if self._ratio_available_servers(time) > self.conf.server_threshold:
            return True
        else:
            return False

    def _ratio_available_servers(self, time):
        return len(self._available_servers(time)) / len(self.servers)

    def _num_available_servers(self, time):
        return len(self._available_servers(time))

    # returns a list of servers not utilized at a given time
    def _available_servers(self, time):
        # Start with all servers as potential servers
        candidate_servers = [s for s in self.servers]
        # remove servers that are busy at the given time
        for t in self.tasks:
            if not (t.start_time <= time and t.end_time > time):
                continue
            for s in t.servers:
                if s in candidate_servers:
                    candidate_servers.remove(s)
        return candidate_servers

    def _schedule_task_given_num_servers(self, num_servers, servers, job, time):
        servers_selec = sample(servers, k=num_servers)
        exec_time = self._exec_time(job.mass, job.alpha, num_servers)
        self.tasks.append(Task(job.id, job.mass, servers_selec, time, time + exec_time))

    def _schedule_task(self, servers, job, time):
        num_servers = min(job.max_num_servers, len(servers))
        servers = sample(servers, k=num_servers)
        exec_time = self._exec_time(job.mass, job.alpha, num_servers)
        self.tasks.append(Task(job.id, job.mass, servers, time, time + exec_time))

    # Returns the possible increase in the number of servers for a task
    # given that num_servers are not busy
    def _task_possible_inc_num_ser(self, task, time, num_servers):
        # job = list(filter(lambda j: (j.id == task.job_id), self.jobs))[0]
        if task.end_time <= time:
            return 0
        job = next(j for j in self.jobs if (j.id == task.job_id))
        task_num_servers = len(task.servers)
        if task_num_servers == job.max_num_servers:
            return 0
        elif task_num_servers + num_servers > job.max_num_servers:
            return job.max_num_servers - task_num_servers
        else:
            return num_servers

    # Formula for communication time
    def _mass_exec(self, alpha, num_serv, exec_time):
        return exec_time * (num_serv) ** alpha

    def _exec_time(self, mass, alpha, num_serv):
        return mass / (num_serv) ** alpha

    # Calculates the reconfiguration time
    def _reconfig_time(self, data, init_servers, final_servers):
        if init_servers > final_servers:
            return data / init_servers * (ceil(init_servers / final_servers) - 1)
        return data / final_servers * (ceil(final_servers / init_servers) - 1)

    ############################################################
    def _jobs_by_mass_remaining(self, time):
        jobs_by_mass = []
        for j in self.jobs:
            if j.sub_time < time:
                m = self._mass_remaining(j, time)
                if m > 0:
                    jobs_by_mass.append([j, m])
        jobs_by_mass = sorted(jobs_by_mass, key=lambda k: k[1], reverse=True)
        return [j[0] for j in jobs_by_mass]

    def _mass_remaining(self, job, time):
        return job.mass - self._mass_executed_at_time(job, time)

    # Finds how much mass has been executing of job at time t
    def _mass_executed_at_time(self, job, time):
        mass_ex = 0
        for t in self.tasks:
            if t.job_id == job.id:
                if t.end_time <= time:
                    mass_ex += t.mass_executed
                else:
                    mass_ex += t.mass_executed - self._mass_exec(
                        job.alpha, len(t.servers), time - t.start_time
                    )
        return mass_ex

    # Finds how much mass has been executing of job
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

    # Returns the job that a task is executing
    def _task_job(self, task):
        return next(j for j in self.jobs if task.from_job(j))

    def stretch_time(self, job):
        # find the termination time from the tasks of that job and subtime, divide the difference by the mass
        term_time = -1
        for t in self.tasks:
            if t.job_id == job.id and t.end_time > term_time:
                term_time = t.end_time
        return (term_time - job.sub_time) / job.mass

    def stretch_times(self):
        # array of all the stretch times
        return [self.stretch_time(j) for j in self.jobs]

    def average_stretch_time(self):
        return mean(self.stretch_times())

    def max_stretch_time(self):
        return max(self.stretch_times())

    def average_power(self):
        work_duration = self.work_duration()
        serv_count = len(self.servers)
        total_energy = 0
        area = 0
        energy_calc = Energy()
        for task in self.tasks:
            task_duration = task.end_time - task.start_time
            task_num_servers = len(task.servers)
            if type(task) is PowerOff:
                total_energy = (
                    total_energy
                    + energy_calc.energy_off(task_duration) * task_num_servers
                )
            else:
                total_energy = (
                    total_energy
                    + energy_calc.energy_computing(task_duration) * task_num_servers
                )
            area = area + task_duration * task_num_servers
        # adding idle time
        energy_idle = (work_duration * serv_count - area) * energy_calc.power_idle
        total_energy = total_energy + energy_idle
        return total_energy

    def normalized_average_power(self):
        work_duration = self.work_duration()
        serv_count = len(self.servers)
        energy_calc = Energy()
        idle_power = work_duration * serv_count * energy_calc.power_idle
        return self.average_power() / idle_power

    def num_reconfig_task(self):
        num = 0
        for t in self.tasks:
            if type(t) is Reconfiguration:
                num = num + 1
        return num

    def num_power_off(self):
        num = 0
        for t in self.tasks:
            if type(t) is PowerOff:
                num = num + 1
        return num

    def cost_function(self):
        return (
            self.conf.stretch_time_weight * self.average_stretch_time()
            + self.conf.energy_weight * self.normalized_average_power()
        )

    def summary(self):
        print("Number of servers: {}".format(len(self.servers)))
        print("Number of jobs scheduled: {}".format(len(self.jobs)))
        print("Number of reconfigurations: {}".format(self.num_reconfig_task()))
        print("Number of power offs: {}".format(self.num_power_off()))
        print("Total work time: {}".format(self.work_duration()))
        print("Cost function value: {}".format(self.cost_function()))

    def stats(self):
        # num reconfig, num power off, min stretch, max stretch, mean stretch, std stretch, av power, cost function
        stretch_times = np.array(self.stretch_times())
        stats = np.array(
            [
                self.num_reconfig_task(),
                self.num_power_off(),
                np.min(stretch_times),
                np.max(stretch_times),
                np.mean(stretch_times),
                np.std(stretch_times),
                self.normalized_average_power(),
                self.cost_function(),
            ]
        )
        return stats

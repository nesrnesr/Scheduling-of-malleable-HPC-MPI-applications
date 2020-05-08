from math import ceil, log, sqrt
from random import randrange, seed, uniform

import numpy
import scipy.stats

from .JobRequest import JobRequest
from .Scheduler import Scheduler


class Experiments:
    def __init__(
        self, reconfig_enable=True, power_off_enable=True, param_enable=True,
    ):
        self.reconfig_enable = reconfig_enable
        self.power_off_enable = power_off_enable
        self.param_enable = param_enable

    def run_expts(self, config, num_srvs, num_expts, seed_num):
        stats = []
        for i in range(num_expts):
            expt_stats = self._run_expt(num_srvs, config, seed_num + i)
            stats.append(expt_stats)
        return stats

    def _run_expt(self, num_srvs, config, seed_num):
        scheduler = Scheduler(
            num_srvs,
            config,
            self.reconfig_enable,
            self.power_off_enable,
            self.param_enable,
        )
        jobs = self._generate_jobs(50, num_srvs, seed_num)

        time = 0
        while jobs or scheduler.is_working():
            for job in list(jobs):
                if time < job.sub_time:
                    break
                scheduler.schedule(job)
                jobs.remove(job)
            scheduler.update_schedule(time)
            time += 10

        scheduler.stop(time)
        return scheduler.stats(stretch_time_weight=1, energy_weight=1)

    def _generate_jobs(self, job_count, server_count, seed_num):
        jobs = []
        previous_sub_time = 0
        for i in range(job_count):
            job = self._generate_job(previous_sub_time, server_count, i, seed_num)
            jobs.append(job)
            previous_sub_time = job.sub_time
        # jobs = sorted(jobs, key=lambda k: k.sub_time)
        return jobs

    def _generate_job(self, timestampLastEvent, server_count, num, seed_num):
        seed(seed_num + num)
        numpy.random.seed(seed=seed_num + num)
        sub_time, mass = self._get_next_task(timestampLastEvent, 500, 1700, 3.8)
        alpha = uniform(0.5, 1)
        data = uniform(10, 500)
        min_num_servers = ceil((alpha / 3) * (server_count - 1))
        max_num_servers = randrange(min_num_servers, server_count)
        return JobRequest(
            "job" + str(num),
            sub_time,
            alpha,
            data,
            mass,
            min_num_servers,
            max_num_servers,
        )

    def _get_next_task(self, timestampLastEvent, dynamism, mass, disparity):
        arrival = scipy.stats.pareto.rvs(4, loc=-1) * 3 * dynamism
        newTimeStamp = timestampLastEvent + arrival
        makespan = self._get_makespan(mass, disparity)
        return (newTimeStamp, makespan)

    def _get_makespan(self, mass, disparity):
        mu = log(mass / disparity)
        sigma = sqrt(2 * (numpy.log(mass) - mu))
        return scipy.stats.lognorm.rvs(sigma, scale=mass / disparity)

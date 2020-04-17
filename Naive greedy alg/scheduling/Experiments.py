from math import ceil
from random import randrange, seed, uniform

from .JobRequest import JobRequest
from .Scheduler import Scheduler, SchedulerConfig


class Experiments:
    def __init__(self, seed = 2):
        self.seed = seed

    def make_random_config(self):
        #seed(self.seed)
        c = SchedulerConfig()
        c.server_threshold = uniform(0.05, 0.2)
        c.ratio_almost_finished_jobs = uniform(0.5, 0.91)
        c.time_remaining_for_power_off = uniform(370, 600)
        c.shut_down_time = uniform(
            c.time_remaining_for_power_off, c.time_remaining_for_power_off * 2
        )
        c.estimated_improv_threshold = uniform(0.5, 0.91)
        c.alpha_min_server_lower_range = uniform(0.01, 0.4)
        c.alpha_min_server_mid_range = uniform(
            c.alpha_min_server_lower_range, c.alpha_min_server_lower_range * 2
        )
        c.alpha_min_server_upper_range = uniform(c.alpha_min_server_mid_range, 1)
        c.alpha_lower = uniform(0.5, 0.7)
        c.alpha_mid = uniform(c.alpha_lower, 0.9)
        return c

    def run_expts(self, config, num_expts):
        seed(self.seed)
        stats = []
        for i in range(num_expts):
            expt_stats = self._run_expt(config)
            stats.append(expt_stats)
        return stats

    def _run_expt(self, config):
        scheduler = Scheduler(config)
        jobs = self._generate_jobs(30, config.server_count)

        time = 0
        while jobs or scheduler.is_working():
            # print(f"time: {time}, {jobs}")
            # sys.stdin.read(1)
            for job in list(jobs):
                if time < job.sub_time:
                    break
                scheduler.schedule(job)
                jobs.remove(job)
            scheduler.update_schedule(time)
            time += 10

        return scheduler.stats()

    # Reconfiguration example
    # def _generate_jobs(self, _, server_count):
    #     a = JobRequest(
    #         id="A",
    #         sub_time=100,
    #         alpha=1,
    #         data=10,
    #         mass=10,
    #         min_num_servers=2,
    #         max_num_servers=server_count,
    #     )
    #
    #     b = JobRequest(
    #         id="B",
    #         sub_time=101,
    #         alpha=1,
    #         data=10,
    #         mass=100,
    #         min_num_servers=2,
    #         max_num_servers=3,
    #     )
    #
    #     c = JobRequest(
    #         id="C",
    #         sub_time=102,
    #         alpha=1,
    #         data=10,
    #         mass=300,
    #         min_num_servers=1,
    #         max_num_servers=3,
    #     )
    #
    #     return [a, b, c]

    def _generate_jobs(self, job_count, server_count):
        jobs = [self._generate_job(server_count, i) for i in range(job_count)]
        jobs = sorted(jobs, key=lambda k: k.sub_time)
        return jobs

    def _generate_job(self, server_count, num):
        sub_time = uniform(0, 3000)
        alpha = uniform(0.5, 1)
        data = uniform(10, 100)
        mass = uniform(10, 5000)
        min_num_servers = ceil(alpha * (server_count - 1))
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

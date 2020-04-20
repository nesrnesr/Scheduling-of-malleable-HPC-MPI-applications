from math import ceil
from random import randrange, uniform

from .JobRequest import JobRequest
from .Scheduler import Scheduler


class Experiments:
    def run_expts(self, config, num_srvs, num_expts):
        stats = []
        for i in range(num_expts):
            expt_stats = self._run_expt(num_srvs, config)
            stats.append(expt_stats)
        return stats

    def _run_expt(self, num_srvs, config):
        scheduler = Scheduler(num_srvs, config)
        jobs = self._generate_jobs(30, num_srvs)

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

        return scheduler.stats(stretch_time_weight=1, energy_weight=1)

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
        jobs = [
            self._generate_job(job_count, server_count, i) for i in range(job_count)
        ]
        jobs = sorted(jobs, key=lambda k: k.sub_time)
        return jobs

    def _generate_job(self, job_count, server_count, num):
        sub_time = uniform(0, job_count * 300)
        alpha = uniform(0.5, 1)
        data = uniform(10, 100)
        mass = uniform(10, 5000)
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

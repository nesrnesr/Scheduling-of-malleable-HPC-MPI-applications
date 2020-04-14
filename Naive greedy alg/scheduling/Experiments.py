from .Job import Job
from .Server import Server
from .Scheduler import Scheduler
import pandas as pd
from random import seed, uniform, randrange


class Experiments:
    STAT_COLS = [
        "num reconfig",
        "num power off",
        "min stretch",
        "max stretch",
        "mean stretch",
        "std stretch",
        "av power",
        "cost function",
    ]

    def __init__(self, num_expts, seeds=2):
        self.num_expts = num_expts
        self.seeds = seeds

    def run_expts(self, config):
        stats = []
        for i in range(self.num_expts):
            expt_stats = self._run_expt(config)
            stats.append(expt_stats)
        return pd.concat(stats)

    def _run_expt(self, config):
        servers = [Server("server" + str(i)) for i in range(10)]
        scheduler = Scheduler(servers, config)
        jobs = self._generate_jobs()

        for t in range(18000):
            time = t * 10
            for job in jobs:
                if job.sub_time > time:
                    break
                scheduler.schedule1(job)
                jobs.remove(job)
            scheduler.update_schedule1(time)

        return pd.DataFrame([scheduler.stats()], columns=self.STAT_COLS)

    def _generate_jobs(self):
        jobs = [self._generate_job(i) for i in range(30)]
        jobs = sorted(jobs, key=lambda k: k.sub_time)
        return jobs

    def _generate_job(self, num):
        seed(self.seeds)
        sub_time = uniform(0, 3000)
        alpha = uniform(0.5, 1)
        data = uniform(10, 1000)
        mass = uniform(10, 50000)
        min_num_servers = randrange(1, 9)
        max_num_servers = randrange(min_num_servers, 10)
        return Job(
            "job" + str(num),
            sub_time,
            alpha,
            data,
            mass,
            min_num_servers,
            max_num_servers,
        )

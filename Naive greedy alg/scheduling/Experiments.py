from .Job import Job
from .Server import Server
from .Scheduler import Scheduler, SchedulerConfig
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

    def __init__(self, seed):
        self.seed = seed

    def make_random_config(self):
        seed(self.seed)
        c = SchedulerConfig()
        c.server_threshold = uniform(0.5, 0.91)
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

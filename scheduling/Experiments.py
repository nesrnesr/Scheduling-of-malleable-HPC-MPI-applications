from math import ceil, log, sqrt
from random import randrange, seed, uniform

import numpy
import scipy.stats

from .JobRequest import JobRequest
from .Scheduler import Scheduler, SchedulerConfig


class Experiments:
    """A simulation environment for running scheduling experiments.
    """

    GENERATED_JOBS_COUNT = 50  #: Number of jobs to generate.

    def __init__(
        self, reconfig_enabled=True, power_off_enabled=True, param_enabled=True,
    ):
        """Constructs an Experiments object.

        Args:
            reconfig_enabled: A flag for enabling reconfigurations.
            power_off_enabled: A flag for enabling power-offs.
            param_enabled: A flag for enabling the decision making process.
        """
        self.reconfig_enabled = reconfig_enabled
        """A flag for enabling reconfigurations."""
        self.power_off_enabled = power_off_enabled  #: A flag for enabling power-offs.
        self.param_enabled = param_enabled  #: A flag for enabling power-offs.

    def run_expts(
        self, config: SchedulerConfig, num_srvs: int, num_expts: int, seed_num: int
    ):
        """Runs a number of experiments with the specified configuration.

        Args:
            config: The configuration the Scheduler within the experiments.
            num_srvs: The total number of servers.
            num_expts: The number of experiements to be run.
            seed_num: A seed used to update the job generator.

        Returns:
            list: A list of scheduling statistics.
        """
        stats = []
        for i in range(num_expts):
            expt_stats = self._run_expt(config, num_srvs, seed_num + i)
            stats.append(expt_stats)
        return stats

    def _run_expt(self, config: SchedulerConfig, num_srvs: int, seed_num: int):
        """Runs one experiment.

        Args:
            config: The configuration the Scheduler within the experiments.
            num_srvs: The total number of servers.
            num_expts: The number of experiements to be run.
            seed_num: A seed used to update the job generator.

        Returns:
            SchedulerStats: A SchedulerStats object wrapping the statistics of\
            within the Experiments.
            By default the weights of the reconfigurations and power-offs in the\
            in the resulting objects are 1.
        """
        scheduler = Scheduler(
            num_srvs,
            config,
            self.reconfig_enabled,
            self.power_off_enabled,
            self.param_enabled,
        )

        jobs = self._generate_jobs(Experiments.GENERATED_JOBS_COUNT, num_srvs, seed_num)

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
        """Generates a set of jobs.

        Args:
            job_count: The number of jobs to be generates.
            server_count: The total number of servers.
            seed_num: A seed used to update the job generator.

        Returns:
            list: A list of generated JobRequest objects.
        """
        jobs = []
        previous_sub_time = 0
        for i in range(job_count):
            job = self._generate_job(previous_sub_time, server_count, i, seed_num)
            jobs.append(job)
            previous_sub_time = job.sub_time
        return jobs

    def _generate_job(self, timestampLastEvent, server_count, num, seed_num):
        """Generates one job.

        Args:
            timestampLastEvent: The time of the last event.
            server_count: The total number of servers.
            num: An index of the job, used to update the job generator.
            seed_num: A seed used to update the job generator.

        Returns:
            list: A list of generated jobs
        """
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

from copy import deepcopy
from dataclasses import astuple, dataclass
from operator import attrgetter, methodcaller
from random import random, sample, uniform
from statistics import mean, stdev

import structlog

from .Job import Job
from .JobRequest import JobRequest
from .Server import Server


@dataclass
class SchedulerConfig:
    """A container for the parameters of the scheduler.
    """

    reconfig_scale: float = 0.331  #: The reconfiguration scaling factor in [0,1].
    reconfig_weight: float = 0.175  #: The reconfiguration weight in [0,1].
    alpha_weight: float = 0.742  #: The speedup factor's weight in [0,1].
    shutdown_scale: float = 0.760  #: The shutdown scaling factor in [0,1].
    shutdown_weight: float = 0.455  #: The shutdown's weight in [0,1].
    shutdown_time_short: float = 899  #: A short duration for shuting the servers.
    shutdown_time_long: float = 1406  #: A long duration for shuting the servers.
    shutdown_time_prob: float = 0.717  #: The probability of choosing shutdown_time_short.

    @classmethod
    def random(cls):
        """Generates random values for the parameters of the scheduler.

        reconfig_scale: is sampled from a Uniform distribution (0.001, 1.0).\n
        reconfig_weight: is sampled from a Uniform distribution (0.01, 1.0).\n
        alpha_weight: is sampled from a Uniform distribution (0.001, 1.0).\n
        shutdown_scale: is sampled from a Uniform distribution (0.001, 1.0).\n
        shutdown_weight: is sampled from a Uniform distribution (0.01, 1.0).\n
        shutdown_time_short: is sampled from a Uniform distribution (370, 1200).\n
        shutdown_time_long: is sampled from a Uniform distribution (370, 4000).\n
        shutdown_time_prob: is sampled from a Uniform distribution (0.0001, 1.0).\n

        """
        c = SchedulerConfig()
        c.reconfig_scale = uniform(0.001, 1.0)
        c.reconfig_weight = uniform(0.01, 1.0)
        c.alpha_weight = uniform(0.001, 1.0)
        c.shutdown_scale = uniform(0.001, 1.0)
        c.shutdown_weight = uniform(0.01, 1.0)
        c.shutdown_time_short = uniform(370, 1200)
        c.shutdown_time_long = uniform(370, 4000)
        c.shutdown_time_prob = uniform(0.0001, 1.0)
        return c

    def to_dict(self):
        """Converts the attributes of a SchedulerConfig object into a dictionary.
        """
        dict_obj = self.__dict__
        return dict_obj

    def to_list(self):
        """Converts the attributes of a SchedulerConfig object into a list.
        """
        return list(astuple(self))


@dataclass
class SchedulerStats:
    """A container for the output statistics of the scheduler.
    """

    complete_jobs: dict  #: The list of the jobs that had been scheduled.
    start_time: int  #: The starting time of the scheduler.
    end_time: int  #: The ending time of the scheduler.
    work_duration: int  #: The span of time during which the scheduling took place.
    reconfig_count: int  #: The total number of reconfigurations that took place.
    power_off_count: int  #: The total number of power-offs that took place.
    min_stretch_time: int  #: The minimum obtained stretch time.
    max_stretch_time: int  #: The maximum obtained stretch time.
    mean_stretch_time: float  #: The mean obtained stretch time.
    stdev_stretch_time: float  #: The standard deviation of the stretch time.
    average_power_norm: float  #: The mean obtained normalized power.
    cost: float  #: The calculated cost resulting from the scheduling of jobs.

    def to_dict(self):
        """Converts the attributes of a SchedulerStats object into a dictionary.

        Discards the list of the completed jobs from the returned dictionary.
        """
        dict_obj = deepcopy(self.__dict__)
        dict_obj.pop("complete_jobs")
        return dict_obj


class Scheduler(object):
    """A representation of a scheduler that assigns Jobs to be run on Servers.
    """

    def __init__(
        self,
        server_count: int,
        conf: SchedulerConfig,
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    ):
        """Creates a Scheduler object.

        Args:
            server_count: The total number of servers in the cluster.
            conf: The configuration of the scheduler.
            reconfig_enabled: A flag for enabling reconfigurations.
            power_off_enabled: A flag for enabling power-offs.
            param_enabled: A flag for enabling the decision taking process,\
            if False the scheduler will always reconfigure jobs, respectively \
            shut down idle servers.

        """
        self.servers = [
            Server(i) for i in range(server_count)
        ]  #: The total number of servers in the cluster.
        self.conf = conf  #: The configuration of the scheduler.
        self.param_enabled = param_enabled
        """A flag for enabling the decision taking process,\
        if False the scheduler will always reconfigure jobs, respectively \
        shut down idle servers."""
        self.reconfig_enabled = reconfig_enabled
        """A flag for enabling reconfigurations."""
        self.power_off_enabled = power_off_enabled  #: A flag for enabling power-offs.
        self.req_queue = []  #: The queue of the scheduler. Holds JobRequest objects.
        self.req_by_id = {}
        """A dictionary where the keys are the ids of the \
         JobRequests objects in the Scheduler's queue. Helps tracking a \
         splitten job due to one or several reconfigurations."""
        self.active_jobs = []  #: A list of all running jobs.
        self.complete_jobs = {}  #: A list of the completed jobs.
        self.logger = structlog.getLogger(__name__)  #: The scheduler's logger.

    def is_working(self):
        """Checks whether the scheduler has finished scheduling.

        Returns:
            True if successful, False otherwise.
        """
        return self.req_queue or (
            self.active_jobs and not all(job.is_power_off() for job in self.active_jobs)
        )

    def stop(self, time):
        """Stops the scheduler at the indicated time.

        Args:
            time: The time at which the scheduler stops working.

        """
        for job in self.active_jobs:
            job.end_time = time
        self._remove_job(*self.active_jobs)

    def schedule(self, job_request: JobRequest):
        """Handles new upcoming JobRequests.

        Args:
            job_request: The JobRequest object to be scheduled by the Scheduler.

        """
        self.req_queue.append(job_request)
        self.req_queue.sort(key=attrgetter("sub_time"), reverse=True)
        self.req_by_id[job_request.id] = job_request

    def update_schedule(self, time):
        """Updates the schedule at time t.

        Args:
            time: The time at which the schedule need to be updated.
        """
        self._remove_job(*[job for job in self.active_jobs if job.is_complete(time)])

        # Schedule jobs in the queue
        av_servers = [server for server in self.servers if not server.is_busy(time)]
        self.logger.debug(
            "update_schedule",
            time=time,
            av_servers=av_servers,
            req_queue=self.req_queue,
        )
        # Priotitize FIFO scheduling as long as there are jobs in the queue
        while self.req_queue and av_servers:
            job_req = self.req_queue[-1]
            job_servers = self._allocate_servers(av_servers, job_req)
            if not job_servers:
                break
            self.logger.debug("schedule job req", time=time, req=job_req)
            job = Job.from_request(job_req, job_servers, start_time=time)
            self._start_job(job)
            self.req_queue.pop()
            av_servers = [server for server in av_servers if server not in job_servers]

        # Applies a reconfiguration
        if self.reconfig_enabled:
            jobs_by_mass = sorted(
                self.active_jobs, key=methodcaller("remaining_mass", time)
            )
            while jobs_by_mass and av_servers:
                job = jobs_by_mass[0]
                if self._is_job_reconfigurable(job, av_servers, time):
                    av_servers = self._reconfigure_job(job, av_servers, time)
                jobs_by_mass.pop(0)

        # Applies power-offs
        if self.power_off_enabled:
            for server in list(av_servers):
                if not self._shutdown_server(av_servers):
                    break

                shutdown, duration = self._allow_shutdown(av_servers)
                if not shutdown:
                    continue

                power_off = Job.make_power_off(
                    [server], start_time=time, duration=duration
                )
                self._start_job(power_off)
                av_servers.remove(server)

    def _allow_shutdown(self, av_servers: list):
        # Shutdown decision process
        if self.param_enabled:
            if (
                0.5
                > ((len(av_servers) / len(self.servers)) ** self.conf.shutdown_weight)
                * self.conf.shutdown_scale
            ):
                return False, 0

            if random() < self.conf.shutdown_time_prob:
                shutdown_duration = self.conf.shutdown_time_short
            else:
                shutdown_duration = self.conf.shutdown_time_long
            return True, shutdown_duration

        else:
            return True, self.conf.shutdown_time_short

    def _start_job(self, *jobs):
        for job in jobs:
            self.active_jobs.append(job)
            self.logger.debug(
                "new job", server_count=len(job.servers), active_jobs=self.active_jobs
            )
            for server in job.servers:
                server.add_job(job)

    def _remove_job(self, *jobs):
        for job in jobs:
            self.active_jobs.remove(job)
            self.logger.debug(f"remove job", job=job, active_jobs=self.active_jobs)
            for server in job.servers:
                server.remove_job(job)

            completed_jobs = self.complete_jobs.get(job.id, [])
            completed_jobs.append(job)
            self.complete_jobs[job.id] = completed_jobs

    def _reconfigure_job(self, job: Job, av_servers: list, time):
        job.interupt(time)
        extra_srv_count = min(job.max_server_count - job.server_count, len(av_servers))
        extra_srvs = sample(av_servers, extra_srv_count)
        job_servers = job.servers + extra_srvs
        av_servers = [server for server in av_servers if server not in extra_srvs]

        self.logger.debug(
            "reconfigure job", time=time, job=job, server_count=len(job_servers)
        )
        reconfig_job, job_rest = job.reconfigure(job_servers, time)
        self._remove_job(job)
        self._start_job(reconfig_job, job_rest)

        return av_servers

    def _is_job_reconfigurable(self, job: Job, av_servers: list, time):
        if not job.is_reconfigurable():
            return False

        extra_srv_count = min(job.max_server_count - job.server_count, len(av_servers))
        # reconfiguration decision process
        if self.param_enabled:
            return (
                0.5
                < (
                    ((len(job.servers) + extra_srv_count) / job.max_server_count)
                    ** self.conf.reconfig_weight
                    * job.alpha ** self.conf.alpha_weight
                )
                * self.conf.reconfig_scale
            )
        else:
            return extra_srv_count > 0

    def _shutdown_server(self, av_servers: list):
        if not self.req_queue:
            return True

        required_servers = sum(req.min_num_servers for req in self.req_queue)
        return len(av_servers) > required_servers

    def _allocate_servers(self, available_servers: list, job_req: JobRequest):
        min_servers = min(job_req.max_num_servers, len(available_servers))
        if min_servers < job_req.min_num_servers:
            return []
        return sample(available_servers, k=min_servers)

    #############################################

    def stats(self, stretch_time_weight: float, energy_weight: float):
        """Updates the schedule at time t.

        Args:
            stretch_time_weight: An exponent weight for the mean stretch time\
             in the cost function.
            energy_weight: An exponent weight for the average normalized power\
             stretch time in the cost function.

        Returns:
            SchedulerStats: A SchedulerStats object is returned.

        """
        stretch_times = self._stretch_times()
        start_time, end_time = self._work_span()
        return SchedulerStats(
            complete_jobs=self.complete_jobs,
            start_time=start_time,
            end_time=end_time,
            work_duration=self._work_duration(),
            reconfig_count=self._num_reconfig_job(),
            power_off_count=self._num_power_off(),
            min_stretch_time=min(stretch_times),
            max_stretch_time=max(stretch_times),
            mean_stretch_time=mean(stretch_times),
            stdev_stretch_time=stdev(stretch_times),
            average_power_norm=self._normalized_average_power(),
            cost=self._cost_function(stretch_times, stretch_time_weight, energy_weight),
        )

    def _work_span(self):
        jobs = [job for jobs_by_id in self.complete_jobs.values() for job in jobs_by_id]
        first = min(jobs, key=attrgetter("start_time"))
        last = max(jobs, key=attrgetter("end_time"))
        return first.start_time, last.end_time

    def _work_duration(self):
        jobs = [job for jobs_by_id in self.complete_jobs.values() for job in jobs_by_id]
        first = min(jobs, key=attrgetter("start_time"))
        last = max(jobs, key=attrgetter("end_time"))
        return last.end_time - first.start_time

    def _stretch_times(self):
        def stretch_time(job_req):
            last = self.complete_jobs[job_req.id][-1]
            return (last.end_time - job_req.sub_time) / job_req.mass

        return [stretch_time(j) for j in self.req_by_id.values()]

    def _normalized_average_power(self):
        def average_power(work_duration):
            jobs = [
                job for jobs_by_id in self.complete_jobs.values() for job in jobs_by_id
            ]
            total_energy = 0
            area = 0
            for job in jobs:
                srv_count = len(job.servers)
                if job.is_power_off():
                    total_energy += Server.Consumption.reboot(job.duration) * srv_count
                else:
                    total_energy += Server.Consumption.active(job.duration) * srv_count
                area += job.duration * srv_count
            # adding idle time
            energy_idle = Server.Consumption.idle(
                work_duration * len(self.servers) - area
            )
            return total_energy + energy_idle

        work_duration = self._work_duration()
        idle_power = Server.Consumption.idle(work_duration) * len(self.servers)
        return average_power(work_duration) / idle_power

    def _num_reconfig_job(self):
        return sum(
            sum(job.is_reconfiguration() for job in jobs)
            for jobs in self.complete_jobs.values()
        )

    def _num_power_off(self):
        return len(self.complete_jobs.get(Job.POWER_OFF_ID, []))

    def _cost_function(self, stretch_times, stretch_time_weight, energy_weight):
        return (
            mean(stretch_times) ** stretch_time_weight
            * self._normalized_average_power() ** energy_weight
        )

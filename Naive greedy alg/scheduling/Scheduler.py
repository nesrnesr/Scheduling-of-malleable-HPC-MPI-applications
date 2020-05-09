from dataclasses import astuple, dataclass
from operator import attrgetter, methodcaller
from random import random, sample, uniform
from statistics import mean, stdev

import structlog

from .Job import Job
from .Server import Server


@dataclass
class SchedulerConfig:
    reconfig_prob: float = 0.331
    reconfig_weight: float = 0.175
    alpha_weight: float = 0.742
    shutdown_prob: float = 0.760
    shutdown_weight: float = 0.455
    shutdown_time_short: float = 899
    shutdown_time_long: float = 1406
    shutdown_time_prob: float = 0.717

    @classmethod
    def random(cls):
        c = SchedulerConfig()
        c.shutdown_prob = uniform(0.001, 1.0)
        c.shutdown_weight = uniform(0.01, 1.0)
        c.reconfig_prob = uniform(0.001, 1.0)
        c.reconfig_weight = uniform(0.01, 1.0)
        c.shutdown_time_short = uniform(370, 1200)
        c.shutdown_time_long = uniform(370, 4000)
        c.shutdown_time_prob = uniform(0.0001, 1.0)
        c.alpha_weight = uniform(0.001, 1.0)
        return c

    def to_dict(self):
        dict_obj = self.__dict__
        return dict_obj

    def to_list(self):
        return list(astuple(self))


@dataclass
class SchedulerStats:
    complete_jobs: dict
    start_time: int
    end_time: int
    work_duration: int
    reconfig_count: int
    power_off_count: int
    min_stretch_time: int
    max_stretch_time: int
    mean_stretch_time: float
    stdev_stretch_time: float
    average_power_norm: float
    cost: float

    def to_dict(self):
        dict_obj = self.__dict__
        dict_obj.pop("complete_jobs")
        return dict_obj


class Scheduler(object):
    def __init__(
        self,
        server_count,
        conf,
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    ):
        self.servers = [Server(i) for i in range(server_count)]
        self.conf = conf
        self.reconfig_enabled = reconfig_enabled
        self.power_off_enabled = power_off_enabled
        self.param_enabled = param_enabled
        self.req_queue = []
        self.req_by_id = {}
        self.active_jobs = []
        self.complete_jobs = {}
        self.logger = structlog.getLogger(__name__)

    def is_working(self):
        return self.req_queue or (
            self.active_jobs and not all(job.is_power_off() for job in self.active_jobs)
        )

    def stop(self, time):
        for job in self.active_jobs:
            job.end_time = time
        self._remove_job(*self.active_jobs)

    def schedule(self, job_request):
        self.req_queue.append(job_request)
        self.req_queue.sort(key=attrgetter("sub_time"), reverse=True)
        self.req_by_id[job_request.id] = job_request

    def update_schedule(self, time):
        self._remove_job(*[job for job in self.active_jobs if job.is_complete(time)])

        # Schedule jobs in the queue
        av_servers = [server for server in self.servers if not server.is_busy(time)]
        self.logger.debug(
            "update_schedule",
            time=time,
            av_servers=av_servers,
            req_queue=self.req_queue,
        )
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

        # Reconfiguration
        if self.reconfig_enabled:
            jobs_by_mass = sorted(
                self.active_jobs, key=methodcaller("remaining_mass", time)
            )
            while jobs_by_mass and av_servers:
                job = jobs_by_mass[0]
                if self._is_job_reconfigurable(job, av_servers, time):
                    av_servers = self._reconfigure_job(job, av_servers, time)
                jobs_by_mass.pop(0)

        # Turn off servers
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

    def _allow_shutdown(self, av_servers):
        if self.param_enabled:
            if (
                0.5
                > ((len(av_servers) / len(self.servers)) ** self.conf.shutdown_weight)
                * self.conf.shutdown_prob
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

    def _reconfigure_job(self, job, av_servers, time):
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

    def _is_job_reconfigurable(self, job, av_servers, time):
        if not job.is_reconfigurable():
            return False

        extra_srv_count = min(job.max_server_count - job.server_count, len(av_servers))
        if self.param_enabled:
            return (
                0.5
                < (
                    ((len(job.servers) + extra_srv_count) / job.max_server_count)
                    ** self.conf.reconfig_weight
                    * job.alpha ** self.conf.alpha_weight
                )
                * self.conf.reconfig_prob
            )
        else:
            return extra_srv_count > 0

    def _shutdown_server(self, av_servers):
        if not self.req_queue:
            return True

        required_servers = sum(req.min_num_servers for req in self.req_queue)
        return len(av_servers) > required_servers

    def _allocate_servers(self, available_servers, job_req):
        min_servers = min(job_req.max_num_servers, len(available_servers))
        if min_servers < job_req.min_num_servers:
            return []
        return sample(available_servers, k=min_servers)

    #############################################

    def stats(self, stretch_time_weight, energy_weight):
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

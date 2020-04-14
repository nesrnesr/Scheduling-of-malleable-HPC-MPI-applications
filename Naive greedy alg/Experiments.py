class Experiments():
    def __init__(self, num_expts, seeds=2):
        self.seeds = seeds
        self.num_expts = num_expts
        self.cols = [
            "num reconfig", "num power off", "min stretch", "max stretch",
            "mean stretch", "std stretch", "av power", "cost function"
        ]
        self.stats = pd.DataFrame(columns=self.cols)
        self.tuning_params = []
        self.scheduler = self.generate_scheduler()

    def generate_job(self, num):
        seed(self.seeds)
        sub_time = uniform(0, 3000)
        alpha = uniform(0.5, 1)
        data = uniform(10, 1000)
        mass = uniform(10, 50000)
        min_num_servers = randrange(1, 9)
        max_num_servers = randrange(min_num_servers, 10)
        return Job('job' + str(num), sub_time, alpha, data, mass,
                   min_num_servers, max_num_servers)

    def generate_jobs(self):
        jobs = [self.generate_job(i) for i in range(30)]
        jobs = sorted(jobs, key=lambda k: k.sub_time)
        return jobs

    def generate_servers(self):
        servers = [Server('server' + str(i)) for i in range(10)]
        return servers

    def generate_scheduler(self):
        scheduler = Scheduler(self.generate_servers())
        return scheduler

    '''server_threshold,ratio_almost_finished_jobs, time_remaining_for_power_off, shut_down_time,
                         estimated_improv_threshold, alpha_min_server_lower_range, alpha_min_server_mid_range,
                         alpha_min_server_upper_range, alpha_lower, alpha_mid'''
    def set_tuning_params(self, tuning_params):
        self.tuning_params = tuning_params
        self.scheduler.set_tuning_params(tuning_params[0], tuning_params[1],
                                         tuning_params[2], tuning_params[3],
                                         tuning_params[4], tuning_params[5],
                                         tuning_params[6], tuning_params[7],
                                         tuning_params[8], tuning_params[9])

    def run_expt(self):
        jobs = self.generate_jobs()

        for t in range(18000):
            time = t * 10
            for job in jobs:
                if job.sub_time <= time:
                    self.scheduler.schedule1(job)
                    jobs.remove(job)
                else:
                    break
            self.scheduler.update_schedule1(time)
        expt_stats = pd.DataFrame([self.scheduler.stats()], columns=self.cols)
        self.stats = pd.concat([self.stats, expt_stats])

    def run_expts(self):
        for i in range(self.num_expts):
            self.run_expt()

    def get_stats(self):
        return self.stats

    def get_mean_cost(self):
        return self.stats['cost function'].mean()

    def get_tuning_params(self):
        return self.tuning_params

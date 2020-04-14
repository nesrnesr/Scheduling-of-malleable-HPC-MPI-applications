from .Experiments import Experiments
from .Scheduler import SchedulerConfig
from random import seed, uniform


def main():
    def generate_random_tuning_config(s):
        seed(s)

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

    experiments = Experiments(num_expts=2)
    config = generate_random_tuning_config(2)
    stats = experiments.run_expts(config)
    print("Experiment stats:\n", stats)
    print("Mean cost: ", stats["cost function"].mean())
    print("Tuning configs:\n", config)

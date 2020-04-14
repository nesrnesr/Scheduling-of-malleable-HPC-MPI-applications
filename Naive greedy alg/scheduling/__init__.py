from .Experiments import Experiments
from random import seed, uniform


def main():
    def generate_random_tuning_params(s):
        seed(s)
        server_threshold = uniform(0.5, 0.91)
        ratio_almost_finished_jobs = uniform(0.5, 0.91)
        time_remaining_for_power_off = uniform(0.5, 0.91)
        shut_down_time = uniform(370, 600)
        estimated_improv_threshold = uniform(shut_down_time, shut_down_time * 2)
        alpha_min_server_lower_range = uniform(0.01, 0.4)
        alpha_min_server_mid_range = uniform(
            alpha_min_server_lower_range, alpha_min_server_lower_range * 2
        )
        alpha_min_server_upper_range = uniform(alpha_min_server_mid_range, 1)
        alpha_lower = uniform(0.5, 0.7)
        alpha_mid = uniform(alpha_lower, 0.9)
        return [
            server_threshold,
            ratio_almost_finished_jobs,
            time_remaining_for_power_off,
            shut_down_time,
            estimated_improv_threshold,
            alpha_min_server_lower_range,
            alpha_min_server_mid_range,
            alpha_min_server_upper_range,
            alpha_lower,
            alpha_mid,
        ]

    experiments = Experiments(2)
    experiments.set_tuning_params(generate_random_tuning_params(2))
    experiments.run_expts()
    print("Experiment stats: ", experiments.get_stats())
    print("Mean cost: ", experiments.get_mean_cost())
    print("Tuning parameters: ", experiments.get_tuning_params())

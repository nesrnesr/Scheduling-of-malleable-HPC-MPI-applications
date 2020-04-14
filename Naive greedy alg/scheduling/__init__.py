from .Experiments import Experiments


def main():
    experiments = Experiments(seed=2)
    config = experiments.make_random_config()
    stats = experiments.run_expts(config, num_expts=2)
    print("Experiment stats:\n", stats)
    print("Mean cost: ", stats["cost function"].mean())
    print("Tuning configs:\n", config)

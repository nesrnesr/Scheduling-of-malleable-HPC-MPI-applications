import logging

import pandas as pd

# from .Swarm import Swarm
from .Experiments import Experiments
from .Scheduler import SchedulerConfig

# python -m scheduling


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)-5s: %(message)s"
    )

    # swarm = Swarm(seed_num=1, num_particles=30, num_srvs=10, num_exp=50)
    # # swarm.configs()
    # swarm.run_epochs(num_epochs=100, draw_stats=False, draw_graph=True)
    # # swarm.configs()

    n_servers = 10
    n_expts = 100
    seed = 2
    # run expt with no reconfig and no power off
    experiment = Experiments(
        reconfig_enabled=False, power_off_enabled=False, param_enabled=False
    )
    configuration = SchedulerConfig
    stats = experiment.run_expts(
        config=configuration, num_srvs=n_servers, num_expts=n_expts, seed_num=seed
    )
    stats_pd = pd.DataFrame([stat.to_dict() for stat in stats])
    stats_pd.to_csv("all_false_stats.csv")
    print("done")
    # Without param
    experiment = Experiments(
        reconfig_enabled=True, power_off_enabled=True, param_enabled=False
    )
    configuration = SchedulerConfig.random()
    stats = experiment.run_expts(
        config=configuration, num_srvs=n_servers, num_expts=n_expts, seed_num=seed
    )
    stats_pd = pd.DataFrame([stat.to_dict() for stat in stats])
    stats_pd.to_csv("withoutparam.csv")
    # run expt with reconfig and no power off
    experiment = Experiments(
        reconfig_enabled=True, power_off_enabled=False, param_enabled=False
    )
    configuration = SchedulerConfig
    stats = experiment.run_expts(
        config=configuration, num_srvs=n_servers, num_expts=n_expts, seed_num=seed
    )
    stats_pd = pd.DataFrame([stat.to_dict() for stat in stats])
    stats_pd.to_csv("reconfig_stats.csv")
    print("done")
    # run expt with reconfig and power off and param from swarm
    experiment = Experiments(
        reconfig_enabled=True, power_off_enabled=True, param_enabled=True
    )
    configuration = SchedulerConfig
    stats = experiment.run_expts(
        config=configuration, num_srvs=n_servers, num_expts=n_expts, seed_num=seed
    )
    stats_pd = pd.DataFrame([stat.to_dict() for stat in stats])
    stats_pd.to_csv("param_stats.csv")
    # Random parameters
    experiment = Experiments(
        reconfig_enabled=True, power_off_enabled=True, param_enabled=True
    )
    configuration = SchedulerConfig.random()
    stats = experiment.run_expts(
        config=configuration, num_srvs=n_servers, num_expts=n_expts, seed_num=seed
    )
    stats_pd = pd.DataFrame([stat.to_dict() for stat in stats])
    stats_pd.to_csv("random.csv")

import logging

import pandas as pd

# from .Swarm import Swarm
from .Experiments import Experiments
from .Scheduler import SchedulerConfig

SERVER_COUNT = 10
EXPTS_COUNT = 100
SEED = 2


def run_experiments(expt_name, scheduler_config, **kwargs):
    experiment = Experiments(**kwargs)
    stats = experiment.run_expts(
        config=scheduler_config,
        num_srvs=SERVER_COUNT,
        num_expts=EXPTS_COUNT,
        seed_num=SEED,
    )
    stats_pd = pd.DataFrame([stat.to_dict() for stat in stats])
    stats_pd.to_csv(f"{expt_name}.csv")


def run_all_experiments():
    run_experiments(
        "all_false_stats",
        SchedulerConfig(),
        reconfig_enabled=False,
        power_off_enabled=False,
        param_enabled=False,
    )

    run_experiments(
        "withoutparam",
        SchedulerConfig.random(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=False,
    )

    run_experiments(
        "reconfig_stats",
        SchedulerConfig(),
        reconfig_enabled=True,
        power_off_enabled=False,
        param_enabled=False,
    )

    run_experiments(
        "param_stats",
        SchedulerConfig(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    )

    run_experiments(
        "random",
        SchedulerConfig.random(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    )


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)-5s: %(message)s"
    )
    run_all_experiments()

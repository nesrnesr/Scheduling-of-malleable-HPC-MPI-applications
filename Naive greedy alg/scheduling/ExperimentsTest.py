import logging

from .Experiments import Experiments
from .Scheduler import SchedulerConfig

logger = logging.getLogger(__name__)

SERVER_COUNT = 10
EXPTS_COUNT = 100
SEED = 2


def run_all_experiments(visualizer):
    output_dir = f"./results/swarm/seed_{SEED}"

    def run_experiments(expt_name, scheduler_config, **kwargs):
        experiment = Experiments(**kwargs)
        stats = experiment.run_expts(
            config=scheduler_config,
            num_srvs=SERVER_COUNT,
            num_expts=EXPTS_COUNT,
            seed_num=SEED,
        )
        visualizer.to_csv(
            [stat.to_dict() for stat in stats], f"{output_dir}/{expt_name}.csv"
        )
        logger.debug("Done.")

    run_experiments(
        "fifo",
        SchedulerConfig(),
        reconfig_enabled=False,
        power_off_enabled=False,
        param_enabled=False,
    )

    run_experiments(
        "fifo_reconfig_poweroff",
        SchedulerConfig.random(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=False,
    )

    run_experiments(
        "fifo_reconfig",
        SchedulerConfig(),
        reconfig_enabled=True,
        power_off_enabled=False,
        param_enabled=False,
    )

    run_experiments(
        "fifo_poweroff",
        SchedulerConfig(),
        reconfig_enabled=False,
        power_off_enabled=True,
        param_enabled=False,
    )

    run_experiments(
        "swarm_param",
        SchedulerConfig(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    )

    run_experiments(
        "random_params",
        SchedulerConfig.random(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    )

import logging
from pathlib import Path

import pandas

from .Experiments import Experiments
from .Scheduler import SchedulerConfig

logger = logging.getLogger(__name__)

# Test file for running the 6 Benchmarking setups in the report.


def load_best_config(seed, path=None):
    if path is None:
        path = f"./results/swarm_training/seed_{seed}/swarm_best_config.csv"
    if not Path(path).exists():
        logger.debug("Specified best config does not exist. Loading default config.")
        return SchedulerConfig()
    result = pandas.read_csv(path)
    result = result.loc[:, ~result.columns.str.contains("^Unnamed")]
    best_config = SchedulerConfig(**result.iloc[0, :].to_dict())
    return best_config


def run_all_experiments(visualizer, config):
    seed = config["SEED"]
    output_dir = f"./results/benchmarking_experiments/seed_{seed}"

    def run_experiments(expt_name, scheduler_config, **kwargs):
        experiment = Experiments(**kwargs)
        stats = experiment.run_expts(
            config=scheduler_config,
            num_srvs=config["SERVER_COUNT"],
            num_expts=config["EXPTS_COUNT"],
            seed_num=config["SEED"],
        )

        if config["draw_experiment_gantt"]:
            for i, stat in enumerate(stats):
                visualizer.draw_gantt(
                    stat, f"{output_dir}/{expt_name}/experiment_{i}.png"
                )
        if config["draw_experiment_cost"]:
            df_stats = pandas.DataFrame([stat.to_dict() for stat in stats])
            visualizer.draw_graph(
                df_stats, f"{output_dir}/{expt_name}/{expt_name}_cost.png"
            )

        visualizer.to_csv(
            [stat.to_dict() for stat in stats],
            f"{output_dir}/{expt_name}/{expt_name}.csv",
        )
        logger.debug("Done.")

    # Reconfigurations and Power-offs take place whenever possible.-------------
    run_experiments(
        "fifo",
        SchedulerConfig(),
        reconfig_enabled=False,
        power_off_enabled=False,
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
        "fifo_reconfig_poweroff",
        SchedulerConfig.random(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=False,
    )

    # Reconfigurations and Power-offs take place after a decision is taken possible.
    run_experiments(
        "random_params",
        SchedulerConfig.random(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    )

    run_experiments(
        "swarm_param",
        load_best_config(seed),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    )

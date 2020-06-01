import argparse
import os

import pandas as pd
import structlog
import yaml

from .ExperimentsTest import run_all_experiments
from .Logging import init as init_logging
from .Swarm import Swarm
from .Visualizer import Visualizer

RESULT_DIR = f"./results/swarm_training/seed_"

logger = structlog.getLogger(__name__)


def run_swarm(visualizer: Visualizer, config: dict):
    """Runs the training of the Swarm.
    Args:
        visualizer: The visualizer object for drawing graphs and charts.
        config: The loaded configuration of the swarm training.
    """
    seed = config["SEED"]

    def draw_stats(num_epoch, particle_idx, exp_stats):
        for i, stat in enumerate(exp_stats):
            visualizer.draw_gantt(
                stat,
                f"{RESULT_DIR}/epoch_{num_epoch}/particule-{particle_idx}-{i}.png",
            )

        df_stat = pd.DataFrame([stat.to_dict() for stat in exp_stats])
        logger.debug(f"\n{df_stat}", epoch=num_epoch, particule_idx=particle_idx)
        logger.debug(
            f"Mean cost",
            cost=df_stat["cost"].mean(),
            epoch=num_epoch,
            particule_idx=particle_idx,
        )

    swarm = Swarm(
        seed_num=seed,
        num_particles=config["PARTICULE_COUNT"],
        num_srvs=config["SERVER_COUNT"],
        num_exp=config["EXPTS_COUNT"],
    )

    stat_handler = draw_stats if config["draw_particle_gantt"] else None
    epoch_costs = swarm.run_epochs(
        num_epochs=config["EPOCH_COUNT"], stat_handler=stat_handler
    )

    if config["draw_cost_graph"]:
        df_cost = pd.DataFrame([cost.to_dict() for cost in epoch_costs])
        visualizer.draw_graph(df_cost, f"{RESULT_DIR}/swarm_cost_graph.png")

    visualizer.to_csv(
        [swarm.best_particle.config.to_dict()], f"{RESULT_DIR}/swarm_best_config.csv"
    )
    visualizer.to_csv(
        [cost.to_dict() for cost in epoch_costs], f"{RESULT_DIR}/swarm_costs.csv"
    )


def get_args(args):
    """Parses the input arguments."""
    parser = argparse.ArgumentParser(
        description="Specifies whether the scheduling operation to perform."
    )
    if args[0].startswith("--train"):
        parser.add_argument(
            "--train-swarm",
            action="store_true",
            required=True,
            help="Initiates the training of a swarm with default parameters",
        )
    else:
        parser.add_argument(
            "--run-benchmarks",
            action="store_true",
            required=True,
            help="Initiates the training of a swarm with default parameters",
        )
    args = parser.parse_args(args=args)
    return args


def load_config():
    """Loads the YAML configuration."""
    stream = open(os.getcwd() + "/config.yml", "r")
    dictionary = yaml.safe_load(stream)
    return dictionary


def main(args):
    init_logging(__name__)
    args = get_args(args)
    visualizer = Visualizer()
    config = load_config()

    if vars(args).get("train_swarm"):
        run_swarm(visualizer, config["swarm"])

    if vars(args).get("run_benchmarks"):
        run_all_experiments(visualizer, config["benchmarks"])

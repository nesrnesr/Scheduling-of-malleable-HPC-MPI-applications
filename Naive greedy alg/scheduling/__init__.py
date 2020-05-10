import pandas as pd
import structlog

from .ExperimentsTest import run_all_experiments
from .Logging import init as init_logging
from .Swarm import Swarm
from .Visualizer import Visualizer

EPOCH_COUNT = 10
PARTICULE_COUNT = 4
SERVER_COUNT = 4
EXPTS_COUNT = 1
SEED = 2
RESULT_DIR = f"./results/swarm/seed_{SEED}"

logger = structlog.getLogger(__name__)


def run_swarm(visualizer, draw_graph=False, draw_exp_stats=False):
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
        seed_num=SEED,
        num_particles=PARTICULE_COUNT,
        num_srvs=SERVER_COUNT,
        num_exp=EXPTS_COUNT,
    )

    stat_handler = draw_stats if draw_exp_stats else None
    epoch_costs = swarm.run_epochs(num_epochs=EPOCH_COUNT, stat_handler=stat_handler)

    if draw_graph:
        df_cost = pd.DataFrame([cost.to_dict() for cost in epoch_costs])
        visualizer.draw_graph(df_cost, f"{RESULT_DIR}/swarm_cost_graph.png")

    visualizer.to_csv(
        [swarm.best_particle.config.to_dict()], f"{RESULT_DIR}/swarm_best_config.csv"
    )
    visualizer.to_csv(
        [cost.to_dict() for cost in epoch_costs], f"{RESULT_DIR}/swarm_costs.csv"
    )


def main():
    init_logging(__name__)
    visualizer = Visualizer()
    # run_all_experiments(visualizer)
    run_swarm(visualizer)

import logging

import pandas as pd

from .Swarm import Swarm
from .Visualizer import Visualizer


def _draw_result(seed, stats, config):
    visualizer = Visualizer()

    for i, stat in enumerate(stats):
        visualizer.draw_gantt(stat, f"./results/result-{seed}-{i}.png")

    df_stat = pd.DataFrame([stat.to_dict() for stat in stats])
    print("Experiment stats:\n", df_stat)
    print("Mean cost: ", df_stat["cost"].mean())
    print("Tuning configs:\n", config)


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)-5s: %(message)s"
    )

    swarm = Swarm(seed_num=1, num_particles=10, num_srvs=10, num_exp=10)
    swarm.configs()
    print(swarm.run_epochs(num_epochs=50))
    swarm.configs()

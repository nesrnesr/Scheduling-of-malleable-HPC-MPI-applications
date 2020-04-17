import logging

import pandas as pd

from .Experiments import Experiments
from .Visualizer import Visualizer
from .Swarm import Swarm


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

    seed = 1
    swarm = Swarm(num_particles = 10, seed = seed)
    swarm.configs()
    print(swarm.run_epochs(num_epochs=50))
    swarm.configs()

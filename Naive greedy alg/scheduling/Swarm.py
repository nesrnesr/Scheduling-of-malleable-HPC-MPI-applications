from math import inf
from random import seed
from statistics import mean, stdev

import pandas as pd

from .Experiments import Experiments
from .Particle import Particle
from .Scheduler import SchedulerConfig
from .Visualizer import Visualizer
from .EpochStats import EpochStats


class Swarm(object):
    def __init__(self, seed_num, num_particles, num_srvs, num_exp=10):
        seed(seed_num)
        self.seed = seed_num
        self.population = [
            Particle(SchedulerConfig.random()) for _ in range(num_particles)
        ]
        self.num_srvs = num_srvs
        self.num_exp = num_exp
        self.best_particle = SchedulerConfig.random()
        self.best_cost = inf
        self.experiment = Experiments()

    def run_epochs(self, num_epochs, draw_stats=False, draw_graph=False):
        epochs_stats = []
        for i in range(num_epochs):
            print("epoch: ", i)
            epochs_stats.append(self._run_epoch(i, draw_stats))
        if draw_graph:
            self._draw_graph(
                num_epochs, pd.DataFrame([stat.to_dict() for stat in epochs_stats])
            )

    def _run_epoch(self, num_epoch, draw_stats):
        self.best_cost = inf
        self.epoch_costs = []
        for i, particle in enumerate(self.population):
            print("Particle: ", i)
            df_configs = particle.config.to_dict()
            print(df_configs)
            stats = self.experiment.run_expts(
                particle.config, num_srvs=self.num_srvs, num_expts=self.num_exp
            )
            if draw_stats:
                self._draw_stats(num_epoch, i, stats, particle.config)
            cost = mean([stat.cost for stat in stats])
            self.epoch_costs.append(cost)
            if cost < self.best_cost:
                self.best_cost = cost
                self.best_particle = particle
            particle.update_cost(cost)

        for particle in self.population:
            particle.update_position(self.best_particle.config)

        print(self.best_cost)
        self.configs()
        return EpochStats(
            num_epoch,
            min(self.epoch_costs),
            max(self.epoch_costs),
            mean(self.epoch_costs),
            stdev(self.epoch_costs),
        )

    def _draw_stats(self, num_epoch, particle_idx, stats, config):
        visualizer = Visualizer()

        for i, stat in enumerate(stats):
            visualizer.draw_gantt(
                stat,
                f"./results/seed_{self.seed}/epoch_{num_epoch}/particule-{particle_idx}-{i}.png",
            )

        df_stat = pd.DataFrame([stat.to_dict() for stat in stats])
        print("Experiment stats:\n", df_stat)
        print("Mean cost: ", df_stat["cost"].mean())
        print("Tuning configs:\n", config)

    def _draw_graph(self, num_epoch, stats):
        visualizer = Visualizer()
        visualizer.draw_graph(
            stats, f"./results/seed_{self.seed}/num_epoch{num_epoch}/graph.png",
        )

    def configs(self):
        df_configs = pd.DataFrame([p.config.to_dict() for p in self.population])
        print("Experiment configs:\n", df_configs)

    # Save stats at each epoch: best/min, max, mean stdv cost

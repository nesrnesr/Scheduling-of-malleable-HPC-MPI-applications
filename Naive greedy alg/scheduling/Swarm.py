import numpy as np
import pandas as pd

from .Experiments import Experiments
from .Particle import Particle
from math import inf



class Swarm(object):
    def __init__(self, num_particles, seed):
        self.num_particles = num_particles
        self.population, self.best_particle = self._init_population()
        self.best_cost = inf
        self.seed = seed


    def run_epochs(self, num_epochs):
        for i in range(num_epochs):
            print("epoch: ", i)
            self._run_epoch()
        return self.best_cost

    def _run_epoch(self):
        self.best_cost = inf
        i=0
        for particle in self.population:
            print("Particle: ", i)
            df_configs = particle.config.to_dict()
            print( df_configs)
            i+=1
            experiments = Experiments(seed = 12)
            stats = experiments.run_expts(particle.config, num_expts = 10)
            df_stat = pd.DataFrame([stat.to_dict() for stat in stats])
            cost = df_stat["cost"].mean()
            if cost < self.best_cost:
                self.best_cost= cost
                self.best_particle = particle
            particle.update_cost(cost)

        for particle in self.population:
            particle.update_position(self.best_particle.config)

        print(self.best_cost)
        self.configs()



    def _init_population(self):
        expt = Experiments()
        return [Particle(expt.make_random_config()) for _ in range(self.num_particles)], expt.make_random_config()

    def configs(self):
        df_configs = pd.DataFrame([p.config.to_dict() for p in self.population])
        print("Experiment configs:\n", df_configs)
    #Save stats at each epoch: best/min, max, mean stdv cost

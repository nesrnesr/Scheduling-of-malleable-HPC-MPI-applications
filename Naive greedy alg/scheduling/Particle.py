from math import inf
from random import random

import numpy as np

from .Scheduler import SchedulerConfig


class Particle:
    def __init__(self, config):
        self.config = config
        self.best_config = config
        self.best_cost = inf
        self.velocity = np.zeros(len(config.to_list()))
        self.c1 = 2
        self.c2 = 2

    def update_position(self, group_best_config):
        position = np.array(self.config.to_list())
        best_pos = np.array(self.best_config.to_list())
        group_best_pos = np.array(group_best_config.to_list())

        self.velocity = 0.1 * (
            self.velocity
            + self.c1 * random() * (best_pos - position)
            + self.c2 * random() * (group_best_pos - position)
        )
        position = self.velocity + position
        self.config = SchedulerConfig(*position)
        self.config = self._check_bounds(self.config)

    def update_cost(self, cost):
        if cost < self.best_cost:
            self.best_cost = cost
            self.best_config = self.config

    def _check_bounds(self, config):
        config.shutdown_prob = self._reflect(config.shutdown_prob, 0, 1)
        config.shutdown_weight = self._reflect(config.shutdown_weight, 0, 1)
        config.reconfig_prob = self._reflect(config.reconfig_prob, 0, 1)
        config.reconfig_weight = self._reflect(config.reconfig_weight, 0, 1)
        config.shutdown_time_short = self._reflect(config.shutdown_time_short, 260, 100000)
        config.shutdown_time_long = self._reflect(config.shutdown_time_long, 260, 100000)
        config.shutdown_time_prob = self._reflect(config.shutdown_time_prob, 0, 1)
        config.alpha_weight = self._reflect(config.alpha_weight, 0, 1)
        return config

    def _reflect(self, variable, lowerbound, upperbound):
        if variable > upperbound:
            return upperbound - (variable - upperbound)
        if variable < lowerbound:
            return lowerbound + (lowerbound - variable)
        return variable

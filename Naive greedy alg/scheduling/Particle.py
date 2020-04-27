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

        print(
            f"position: {len(position)} {self.config} {self.config.to_list()}  {position}"
        )
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
        config.shutdown_time_1 = self._reflect(config.shutdown_time_1, 260, 100000)
        config.shutdown_time_2 = self._reflect(config.shutdown_time_2, 260, 100000)
        config.shutdown_time_prob = self._reflect(config.shutdown_time_prob, 0, 1)
        config.alpha_min_server_lower_range = self._reflect(
            config.alpha_min_server_lower_range, 0, 1
        )
        config.alpha_min_server_mid_range = self._reflect(
            config.alpha_min_server_mid_range, config.alpha_min_server_lower_range, 1
        )
        config.alpha_min_server_upper_range = self._reflect(
            config.alpha_min_server_upper_range, config.alpha_min_server_mid_range, 1
        )
        config.alpha_lower = self._reflect(config.alpha_lower, 0.5, 1)
        config.alpha_mid = self._reflect(config.alpha_mid, config.alpha_lower, 1)

        # config.shutdown_prob = min(max(config.shutdown_prob, 0.000001), 0.999999)
        # config.shutdown_weight = min(max(config.shutdown_weight, 0.000001), 0.999999)
        # config.reconfig_prob = min(max(config.reconfig_prob, 0.000001), 0.999999)
        # config.reconfig_weight = min(max(config.reconfig_weight, 0.000001), 0.999999)
        # config.shutdown_time_1 = max(config.shutdown_time_1, 10)
        # config.shutdown_time_2 = max(config.shutdown_time_2, 10)
        # config.shutdown_time_prob = min(
        #     max(config.shutdown_time_prob, 0.000001), 0.999999
        # )
        # config.alpha_min_server_lower_range = min(
        #     max(config.alpha_min_server_lower_range, 0.000001), 0.91
        # )
        # config.alpha_min_server_mid_range = min(
        #     max(
        #         config.alpha_min_server_mid_range,
        #         config.alpha_min_server_lower_range + 0.001,
        #     ),
        #     0.91,
        # )
        # config.alpha_min_server_upper_range = min(
        #     max(
        #         config.alpha_min_server_upper_range,
        #         config.alpha_min_server_mid_range + 0.001,
        #     ),
        #     0.99,
        # )
        # config.alpha_lower = min(max(config.alpha_lower, 0.5), 0.91)
        # config.alpha_mid = min(
        #     max(config.alpha_mid, config.alpha_lower + 0.001,), 0.99,
        # )
        return config

    def _reflect(self, variable, lowerbound, upperbound):
        if variable > upperbound:
            return upperbound - (variable - upperbound)
        if variable < lowerbound:
            return lowerbound + (lowerbound - variable)
        return variable

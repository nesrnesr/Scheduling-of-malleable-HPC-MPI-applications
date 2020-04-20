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

        self.velocity = (
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
        config.shutdown_prob = min(max(config.shutdown_prob, 0.05), 0.91)
        config.shutdown_weight = min(max(config.shutdown_weight, 0.05), 0.91)
        config.reconfig_prob = min(max(config.reconfig_prob, 0.05), 0.91)
        config.reconfig_weight = min(max(config.reconfig_weight, 0.05), 0.91)
        config.shutdown_time = max(config.shutdown_time, 360)

        # config.alpha_min_server_lower_range = min(max(config.alpha_min_server_lower_range, 0.01), 0.9)
        # config.server_threshold = min(max(config.server_threshold, 0.05), 0.9)
        # config.server_threshold = min(max(config.server_threshold, 0.05), 0.9)

        if config.alpha_min_server_lower_range < 0:
            config.alpha_min_server_lower_range = 0.01
        if config.alpha_min_server_lower_range > 1:
            config.alpha_min_server_lower_range = 0.91
        if config.alpha_min_server_mid_range < config.alpha_min_server_lower_range:
            config.alpha_min_server_mid_range = (
                config.alpha_min_server_lower_range + 0.1
            )
        if config.alpha_min_server_mid_range > 1:
            config.alpha_min_server_mid_range = 0.91
        if config.alpha_min_server_upper_range < config.alpha_min_server_mid_range:
            config.alpha_min_server_upper_range = (
                config.alpha_min_server_mid_range + 0.1
            )
        if config.alpha_min_server_upper_range > 1:
            config.alpha_min_server_upper_range = 0.99
        if config.alpha_lower < 0.5:
            config.alpha_lower = 0.5
        if config.alpha_lower > 1:
            config.alpha_lower = 0.91
        if config.alpha_mid < config.alpha_lower:
            config.alpha_mid = config.alpha_lower + 0.01
        if config.alpha_mid > 1:
            config.alpha_mid = 0.99
        return config

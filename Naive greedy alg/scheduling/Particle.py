from .Scheduler import SchedulerConfig
from random import random
from math import inf
import numpy as np

class Particle():
    def __init__(self, config):
        self.config = config
        self.best_config = self.config
        self.best_cost = inf
        self.velocity = np.zeros(10)
        self.c1 = 2
        self.c2 = 2

    def update_position(self, group_best_config):
        position = self._config_to_pos(self.config)
        best_pos = self._config_to_pos(self.best_config)
        group_best_pos  = self._config_to_pos(group_best_config)

        self.velocity = self.velocity + self.c1 * random() * (best_pos  - position) + self.c2 * random() * (group_best_pos - position)
        position = self.velocity + position

        self.config = self._pos_to_config(position)
        self.config = self._check_bounds(self.config)

    def update_cost(self, cost):
        if cost < self.best_cost:
            self.best_cost = cost
            self.best_config = self.config

    def _pos_to_config(self, position):
        c = SchedulerConfig()
        c.server_threshold = position[0]
        c.ratio_almost_finished_jobs = position[1]
        c.time_remaining_for_power_off = position[2]
        c.shut_down_time = position[3]
        c.estimated_improv_threshold = position[4]
        c.alpha_min_server_lower_range = position[5]
        c.alpha_min_server_mid_range = position[6]
        c.alpha_min_server_upper_range = position[7]
        c.alpha_lower = position[8]
        c.alpha_mid = position[9]
        return c

    def _config_to_pos(self, config):
        return np.array([
        config.server_threshold,
        config.ratio_almost_finished_jobs,
        config.time_remaining_for_power_off,
        config.shut_down_time,
        config.estimated_improv_threshold,
        config.alpha_min_server_lower_range,
        config.alpha_min_server_mid_range,
        config.alpha_min_server_upper_range,
        config.alpha_lower,
        config.alpha_mid
        ])

    def _check_bounds(self, config):
        if config.server_threshold < 0:
            config.server_threshold = 0.05
        if config.server_threshold > 0.9:
            config.server_threshold = 0.9
        if config.ratio_almost_finished_jobs < 0:
            config.ratio_almost_finished_jobs = 0.05
        if config.ratio_almost_finished_jobs > 1:
            config.ratio_almost_finished_jobs = 0.91
        if config.time_remaining_for_power_off < 360:
            config.time_remaining_for_power_off = 360
        if config.shut_down_time < 360:
            config.shut_down_time = 360
        if config.estimated_improv_threshold < 0:
            config.estimated_improv_threshold = 0.05
        if config.estimated_improv_threshold > 1:
            config.estimated_improv_threshold = 0.91
        if config.alpha_min_server_lower_range < 0:
            config.alpha_min_server_lower_range = 0.01
        if config.alpha_min_server_lower_range > 1:
            config.alpha_min_server_lower_range = 0.91
        if config.alpha_min_server_mid_range < config.alpha_min_server_lower_range:
            config.alpha_min_server_mid_range = config.alpha_min_server_lower_range+0.1
        if config.alpha_min_server_mid_range > 1:
            config.alpha_min_server_mid_range = 0.91
        if config.alpha_min_server_upper_range < config.alpha_min_server_mid_range:
            config.alpha_min_server_upper_range = config.alpha_min_server_mid_range+0.1
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

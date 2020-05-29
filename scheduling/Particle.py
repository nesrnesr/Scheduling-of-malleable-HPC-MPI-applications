from math import inf
from random import random

import numpy as np

from .Scheduler import SchedulerConfig


class Particle:
    """A representative class of a member of the Swarm.
    """

    def __init__(self, config: SchedulerConfig):
        """Constructs a Particle objects

        Args:
            config: The scheduler configuration the Particle \
            would use for scheduling jobs.
        """

        self.config = config
        """SchedulerConfig: The configuration within the Particle."""
        self.best_config = config
        """SchedulerConfig: The best configuration within the Particle."""
        self.best_cost = inf
        """float: The best cost the Particle calculated."""
        self.velocity = np.zeros(len(config.to_list()))
        """numpy.array: The velocity vector of the Particle."""
        self.c1 = 2
        """A scaling factor for the relative position of the Particle in respect\
         to its best known position."""
        self.c2 = 2
        """A scaling factor for the relative position of the Particle in respect\
         to the group's best known position."""
        self.pace = 0.1  #: The updating pace

    def update_position(self, group_best_config: SchedulerConfig):
        """Updates the position of the Particle.

        Args:
            group_best_config (SchedulerConfig): The Swarm best known configuration.
        """
        position = np.array(self.config.to_list())
        best_pos = np.array(self.best_config.to_list())
        group_best_pos = np.array(group_best_config.to_list())

        self.velocity = self.pace * (
            self.velocity
            + self.c1 * random() * (best_pos - position)
            + self.c2 * random() * (group_best_pos - position)
        )
        position = self.velocity + position
        self.config = SchedulerConfig(*position)
        self.config = self._check_bounds(self.config)

    def update_cost(self, cost: float):
        """Updates the best cost of the Particle.

        Args:
            cost: The calculated cost to compare to the Particle's best cost.
        """
        if cost < self.best_cost:
            self.best_cost = cost
            self.best_config = self.config

    def _check_bounds(self, config):
        # Handles boundaries checks through the reflection method
        config.shutdown_scale = self._reflect(config.shutdown_scale, 0, 1)
        config.shutdown_weight = self._reflect(config.shutdown_weight, 0, 1)
        config.reconfig_scale = self._reflect(config.reconfig_scale, 0, 1)
        config.reconfig_weight = self._reflect(config.reconfig_weight, 0, 1)
        config.shutdown_time_short = self._reflect(
            config.shutdown_time_short, 260, 100000
        )
        config.shutdown_time_long = self._reflect(
            config.shutdown_time_long, 260, 100000
        )
        config.shutdown_time_prob = self._reflect(config.shutdown_time_prob, 0, 1)
        config.alpha_weight = self._reflect(config.alpha_weight, 0, 1)
        return config

    def _reflect(self, variable, lowerbound, upperbound):
        if variable > upperbound:
            return upperbound - (variable - upperbound)
        if variable < lowerbound:
            return lowerbound + (lowerbound - variable)
        return variable

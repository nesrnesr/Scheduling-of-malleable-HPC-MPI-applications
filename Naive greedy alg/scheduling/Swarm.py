from dataclasses import dataclass
from random import seed
from statistics import mean, stdev

from .Experiments import Experiments
from .Particle import Particle
from .Scheduler import SchedulerConfig


@dataclass
class EpochCost:
    epoch: int
    min: float
    max: float
    mean: float
    std: float

    @classmethod
    def from_costs(cls, epoch, particules_cost):
        return cls(
            epoch,
            min(particules_cost),
            max(particules_cost),
            mean(particules_cost),
            stdev(particules_cost),
        )

    def to_dict(self):
        dict_obj = self.__dict__
        return dict_obj


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
        self.experiment = Experiments()

    def run_epochs(self, num_epochs, stat_handler):
        epochs_costs = []
        for i in range(num_epochs):
            epoch_cost = self._run_epoch(i, stat_handler)
            epochs_costs.append(epoch_cost)
        return epochs_costs

    def _run_epoch(self, num_epoch, stat_handler):
        particules_exp_stats = []
        particules_cost = []
        best_cost = None

        for i, particle in enumerate(self.population):
            stats = self.experiment.run_expts(
                particle.config,
                num_srvs=self.num_srvs,
                num_expts=self.num_exp,
                seed_num=num_epoch,
            )
            if stat_handler is not None:
                stat_handler(num_epoch, i, stats)
            particules_exp_stats.append(stats)
            cost = mean([stat.cost for stat in stats])
            particules_cost.append(cost)
            if best_cost is None or cost < best_cost:
                best_cost = cost
                self.best_particle = particle
            particle.update_cost(cost)

        for particle in self.population:
            particle.update_position(self.best_particle.config)

        return EpochCost.from_costs(num_epoch, particules_cost)

    # def _configs(self):
    #     df_configs = pd.DataFrame([p.config.to_dict() for p in self.population])
    #     print("Experiment configs:\n", df_configs)

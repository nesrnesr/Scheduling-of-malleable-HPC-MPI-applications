import logging

from .Swarm import Swarm


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)-5s: %(message)s"
    )

    swarm = Swarm(seed_num=1, num_particles=10, num_srvs=10, num_exp=10)
    # swarm.configs()
    swarm.run_epochs(num_epochs=1, draw_stats=True)
    # swarm.configs()

import pandas as pd
import structlog

# from .Swarm import Swarm
from .Experiments import Experiments
from .Scheduler import SchedulerConfig

SERVER_COUNT = 10
EXPTS_COUNT = 100
SEED = 2

logger = structlog.getLogger(__name__)


def run_all_experiments():
    run_experiments(
        "all_false_stats",
        SchedulerConfig(),
        reconfig_enabled=False,
        power_off_enabled=False,
        param_enabled=False,
    )

    run_experiments(
        "withoutparam",
        SchedulerConfig.random(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=False,
    )

    run_experiments(
        "reconfig_stats",
        SchedulerConfig(),
        reconfig_enabled=True,
        power_off_enabled=False,
        param_enabled=False,
    )

    run_experiments(
        "param_stats",
        SchedulerConfig(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    )

    run_experiments(
        "random",
        SchedulerConfig.random(),
        reconfig_enabled=True,
        power_off_enabled=True,
        param_enabled=True,
    )


def main():
    init_logging(__name__)
    visualizer = Visualizer()
    # run_all_experiments(visualizer)
    run_swarm(visualizer)

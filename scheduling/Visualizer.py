from dataclasses import astuple, dataclass
from pathlib import Path
from random import uniform

import matplotlib.pyplot as plt
import pandas as pd
import structlog

from .Scheduler import SchedulerStats
from .Swarm import EpochCost


@dataclass
class Vector2i:
    """A 2-dimensional vector of int."""

    x: int = 0  #: value on the x axis.
    y: int = 0  #: value on the y axis.


@dataclass
class Color:
    """A color container object.

    Default color is black.
    """

    r: float = 0  #: Normalized red channel value.
    g: float = 0  #: Normalized green channel value.
    b: float = 0  #: Normalized blue channel value.
    a: float = 1  #: Normalized alpha value (influences the transparency).


logger = structlog.getLogger(__name__)


class Visualizer:
    """A class that creates different types of visualizations.
    """

    def draw_gantt(self, stats: SchedulerStats, filepath: str):
        """Draws a Gantt chart.

        Directories referenced by filepath are created similar to mkdir -p.

        Args:
            stats (SchedulerStats): A container object for the scheduler's \
            output statistics.
            filepath (str): The location for writing the resulting Gantt chart.

        """
        path = Path(filepath)
        path.parent.mkdir(0o755, parents=True, exist_ok=True)

        plt.figure(filepath)
        plt.subplot()

        power_off_color = Color(0, 0, 0)
        for jobs in stats.complete_jobs.values():
            job_color = Color(
                uniform(0.25, 0.9), uniform(0.25, 0.9), uniform(0.25, 0.9)
            )
            for job in jobs:
                job_color.a = 0.5 if job.is_reconfiguration() else 1
                if job.is_power_off():
                    job_color = power_off_color
                for server in job.servers:
                    tl = Vector2i(job.start_time, server.index)
                    size = Vector2i(job.duration, 1)
                    self._draw_rectangle(tl=tl, size=size, color=job_color)

        plt.ylabel("servers")
        plt.xlabel("time")
        plt.axis("auto")
        plt.savefig(filepath, dpi=200)
        plt.close(filepath)

    def draw_graph(self, stats, filepath: str):
        """Draws a 2D graph of the mean cost against the epoch count.

        Directories referenced by filepath are created similar to mkdir -p.

        Args:
            stats: A container object for the epoch's statistics.
            filepath (str): Location for writing the resulting chart.

        """
        path = Path(filepath)
        path.parent.mkdir(0o755, parents=True, exist_ok=True)
        fig, ax = plt.subplots(1)
        ax.plot(stats["epoch"], stats["mean"], lw=2, color="blue")
        ax.legend(loc="upper right")
        ax.set_xlabel("Epoch")
        ax.set_ylabel("Mean cost")
        plt.savefig(filepath, dpi=200)

    def to_csv(self, table: list, path: str):
        """Converts a list into a csv file.

        Directories referenced by filepath are created similar to mkdir -p.

        Args:
            table (list): A python list.
            path (str): The location for writing the csv file.

        """
        path = Path(path)
        path.parent.mkdir(0o755, parents=True, exist_ok=True)
        df_table = pd.DataFrame(table)
        logger.debug(
            f"Obtained costs, stored in {path.name}:\n{df_table}", name=path.name
        )
        df_table.to_csv(path)

    def _draw_rectangle(self, tl, size, color):
        rectangle = plt.Rectangle((tl.x, tl.y), size.x, size.y, fc=astuple(color))
        plt.gca().add_patch(rectangle)

from dataclasses import astuple, dataclass
from pathlib import Path
from random import uniform

import matplotlib.pyplot as plt


@dataclass
class Vector2i:
    x: int = 0
    y: int = 0


@dataclass
class Color:
    r: float
    g: float
    b: float
    a: float = 1


class Visualizer:
    def draw_gantt(self, stats, filepath):
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

    def draw_graph(self, stats, filepath):
        path = Path(filepath)
        path.parent.mkdir(0o755, parents=True, exist_ok=True)
        fig, ax = plt.subplots(1)
        ax.plot(stats["epoch"], stats["mean_cost"], lw=2, color="blue")
        # ax.fill_between(
        #     stats["epoch"],
        #     stats["min_cost"],
        #     stats["max_cost"],
        #     facecolor="blue",
        #     alpha=0.1,
        # )
        # ax.fill_between(
        #     stats["epoch"],
        #     stats["mean_cost"] + stats["std_cost"],
        #     stats["mean_cost"] - stats["std_cost"],
        #     facecolor="blue",
        #     alpha=0.4,
        # )
        ax.legend(loc="upper right")
        ax.set_xlabel("Epoch")
        ax.set_ylabel("Mean cost")
        plt.savefig(filepath, dpi=200)

    def _draw_rectangle(self, tl, size, color):
        rectangle = plt.Rectangle((tl.x, tl.y), size.x, size.y, fc=astuple(color))
        plt.gca().add_patch(rectangle)

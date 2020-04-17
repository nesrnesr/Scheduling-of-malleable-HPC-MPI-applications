from dataclasses import astuple, dataclass
from pathlib import Path
from random import random

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

        power_off_color = Color(0, 0, 0)
        for jobs in stats.complete_jobs.values():
            job_color = Color(random(), random(), random())
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

    def _draw_rectangle(self, tl, size, color):
        rectangle = plt.Rectangle((tl.x, tl.y), size.x, size.y, fc=astuple(color))
        plt.gca().add_patch(rectangle)

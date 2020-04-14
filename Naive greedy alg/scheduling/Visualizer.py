from dataclasses import dataclass
import cairocffi as cairo


@dataclass
class Vector2i():
    x: int = 0
    y: int = 0


class Visualizer():
    def __init__(self, width=800, height=800, x_offset=0.1, y_offset=0.1):
        self.height = height
        self.width = width
        self.x_offset = x_offset
        self.y_offset = y_offset
        self.surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, height, width)
        self.context = cairo.Context(self.surface)

    def draw_gantt(self, scheduler, filename, grid_draw=False):
        self._setup_canvas()
        max_time = self._setup_xaxis(scheduler.work_duration())
        num_servers = self._setup_yaxis(scheduler.server_count)
        width_scaling = self._scale_axis(self.x_offset, max_time)
        height_scaling = self._scale_axis(self.y_offset, num_servers)

        jobs_colors = {
            j_id: self._generate_color()
            for j_id in scheduler.job_ids()
        }

        ids = scheduler.server_ids()
        servers_ypos = {
            id: ypos
            for id, ypos in zip(ids, self._server_yposition(ids))
        }

        for task in scheduler.tasks:
            if type(task) is PowerOff:
                r_col = 0
                g_col = 0
                b_col = 0
            else:
                j_id = task.job_id
                r_col = jobs_colors[j_id][0]
                g_col = jobs_colors[j_id][1]
                b_col = jobs_colors[j_id][2]
            t_time = task.end_time - task.start_time
            for server in task.servers:
                alpha = 0.5 if isinstance(task, Reconfiguration) else 1.0
                tl = Vector2i(self.x_offset + task.start_time * width_scaling,
                              servers_ypos[server.id])
                size = Vector2i(width_scaling * t_time, height_scaling)
                self._draw_rectangle(tl=tl,
                                     size=size,
                                     r=r_col,
                                     g=g_col,
                                     b=b_col,
                                     alpha=alpha)

        if (grid_draw):
            self._draw_grid(scheduler.work_duration(), num_servers)

        self.context.set_font_size(0.05)
        self._set_labels(xlabel="Time",
                         xlabel_pos=Vector2i(0.8, 0.97),
                         ylabel="Servers",
                         ylabel_pos=Vector2i(0, 0.07))
        self._save_to(filename)

    def _setup_canvas(self, height=800, width=800):
        self._normalize_canvas()
        self._draw_rectangle()

    def _normalize_canvas(self):
        self.context.scale(self.height, self.width)

    def _setup_xaxis(self, xmax):
        return 1.01 * xmax

    def _setup_yaxis(self, ymax):
        return ymax

    def _scale_axis(self, offset, max_value):
        return (1 - offset * 2) / max_value

    def _generate_color(self):
        return [random() for j in range(3)]

    def _server_yposition(self, servers):
        y_positions = []
        server_count = len(servers)
        height_scaling = self._scale_axis(self.y_offset, server_count)
        for i in range(server_count):
            y_positions.append(self.y_offset + i * height_scaling)
        return y_positions

    def _draw_rectangle(self,
                        tl=Vector2i(),
                        size=Vector2i(1, 1),
                        r=1,
                        g=1,
                        b=1,
                        alpha=1.0):
        self.context.rectangle(tl.x, tl.y, size.x, size.y)
        self._set_color(r, g, b, alpha)
        self.context.fill()
        self.x_offset = x_offset

    def _set_color(self, r=0, g=0, b=0, alpha=1.0):
        self.context.set_source_rgba(r, g, b, alpha)

    def _draw_grid(self, xmax, ymax):
        scaled_x = self._setup_xaxis(xmax)
        width_scaling = self._scale_axis(self.x_offset, scaled_x)
        height_scaling = self._scale_axis(self.y_offset, ymax)

        for i in range(ceil((xmax + 1) / 1000)):
            pos = Vector2i(self.x_offset + (i * width_scaling) * 1000,
                           self.y_offset)
            self._move_cursor(pos)
            pos.y = 1 - pos.y
            self._draw_line(pos)

        for j in range(ymax + 1):
            pos = Vector2i(self.x_offset, self.y_offset + j * height_scaling)
            self._move_cursor(pos)
            pos.x += xmax
            self._draw_line(pos)

    def _move_cursor(self, position):
        self.context.move_to(position.x, position.y)

    def _draw_line(self, position, line_width=0.001, r=0, g=0, b=0, alpha=0.9):
        self.context.line_to(position.x, position.y)
        self._set_color(r, g, b, alpha)
        self.context.set_line_width(line_width)
        self.context.stroke()

    def _set_labels(self, xlabel, xlabel_pos, ylabel, ylabel_pos):
        self._move_cursor(xlabel_pos)
        self.context.show_text(xlabel)

        self._move_cursor(ylabel_pos)
        self.context.show_text(ylabel)

    def _save_to(self, filename):
        self.surface.write_to_png(filename)

from .Task import Task


class PowerOff(Task):
    def __init__(self, servers, start_time, end_time):
        super(PowerOff, self).__init__("power off", 0, servers, start_time, end_time)

    def __repr__(self):
        return "Power off, #servers: {} start_time: {}, end_time: {}".format(
            len(self.servers), self.start_time, self.end_time
        )

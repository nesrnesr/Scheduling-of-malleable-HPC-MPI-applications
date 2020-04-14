class Power_off(Task):
    def __init__(self, servers, start_time, end_time):
        Task.__init__(self, "power off", 0, servers, start_time, end_time)

    def __str__(self):
        return "Power off, #servers: {} start_time: {}, end_time: {}".format(
            len(self.servers), self.start_time, self.end_time)

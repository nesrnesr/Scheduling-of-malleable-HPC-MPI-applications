class Job():
    def __init__(self, id, sub_time, alpha, data, mass, min_num_servers,
                 max_num_servers):
        self.id = id
        self.sub_time = sub_time
        self.alpha = alpha
        self.data = data
        self.mass = mass
        self.min_num_servers = min_num_servers
        self.max_num_servers = max_num_servers

    def __eq__(self, other):
        return self.id == other.id

class Energy():
    def power_idle(self):
        return 95

    def energy_idle(self, time):
        return self.power_idle() * time

    def energy_computing(self, time):
        return 191 * time

    def power_off_on(self):
        return 125

    def time_off_on(self):
        return 151

    def power_on_off(self):
        return 101

    def time_on_off(self):
        return 6

    def power_off(self):
        return 10

    def energy_off(self, time):
        off_time = time - self.time_off_on() - self.time_on_off()
        energy = self.power_off_on() * self.time_off_on() + self.power_on_off(
        ) * self.time_on_off()
        energy = energy + off_time * self.power_off()
        return energy

from dataclasses import dataclass


@dataclass
class Energy():
    power_idle: int = 95
    power_off_on: int = 125
    power_on_off: int = 101
    time_off_on: int = 151
    time_on_off: int = 6
    power_off: int = 10
    active_server_consumption: int = 191

    def energy_idle(self, time):
        return self.power_idle * time

    def energy_computing(self, time):
        return self.active_server_consumption * time

    def energy_off(self, time):
        off_time = time - self.time_off_on - self.time_on_off
        energy = self.power_off_on * self.time_off_on + self.power_on_off * self.time_on_off
        energy += off_time * self.power_off
        return energy

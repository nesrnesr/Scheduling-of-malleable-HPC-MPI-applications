# Scheduling of malleable HPC applications using a Particle Swarm optimised greedy algorithm

<div align="center">
  <img src="./assets/images/malleable-jobs-poff.png" alt="banner">
</div>
<h1 align="center" style="font-size: 50px; color:#000; font-weight: 600"></h1>


This project is a prototype of a simulator supporting the scheduling of [malleable](https://dl.acm.org/doi/10.1109/SBAC-PAD.2004.27) jobs along with the shutting down of idle servers for energy saving. Similar works have tackled seperately both the problematics of using malleable jobs to improve the efficiency schedulers on one side, and the data centers [energy efficiency](https://www.researchgate.net/publication/311756668_Energy_Aware_Dynamic_Provisioning_for_Heterogeneous_Data_Centers) on the other side. In our approach we combine both goals to find a solution that optimises both objectives.

### Background
This project started as academic research project for the course of Optimisation for Computer Science by Dr. Pascal BOUVRY and Dr. Grégoire DANOY from the University of Luxembourg. It was supervised by Dr. Georges Da Costa from the University of Toulouse. It was made by three [MICS](https://wwwfr.uni.lu/formations/fstm/master_in_information_and_computer_sciences) students: Nesryne Mejri, Briag Dupont and Lucas Romao.

General goals of this project are:
- Using a metaheuristic approach to find a solution that optimises the multi-objective cost function.
- Providing an environment that supports the scheduling of malleable jobs.
- Using the scientific approach to analyse the obtained results.

### Features
The project implements a greedy algorithm that schedules jobs in a FIFO fashion.
It also makes use of the Particle Swarm Optimisation to tune the scheduler and let it make decisions of whether to reconfigure a job or to shut down an idle server.

Other features:
- A realistic job generator based on Google data centers distribution from [here](https://www.researchgate.net/publication/315848929_Modeling_and_generating_large-scale_Google-like_workload).
- Drawing of the scheduling results as Gantt charts.
- Allowing job reconfigurations from a lower to a higher number of servers.
- Automatic documentation generation.
- Dependency management using [Poetry](https://python-poetry.org/)

### How to install
- Clone this project and enter the directory.
- Execute the setup script by using `./setup.sh`

#### Use
Use the command `./launcher.sh start --help`. and select which operation to run\
To generate documentation use `./launcher.sh gendoc`

##### Changing parameters.
To produces visuals and/or change the parameters of either the swarm training or the benchmarking experiments, modify the `config.yml` file


### License
This project is distributed under the [MIT License](https://raw.githubusercontent.com/briagd/Scheduling-of-malleable-HPC-MPI-applications/master/LICENSE)

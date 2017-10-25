# PPT-SCHED : Simian PPT(Performance Prediction Toolkit) Job Scheduler


*Simulation of HPC Job Scheduling and Large-Scale Parallel Workloads, Mohammad Abu Obaida and Jason Liu. In Proceedings of the 2017 Winter Simulation Conference (WSC 2017), [To appear]*

This repository is the public facing code base of parallel workloads scheduling simulator. 
The simulator was developed based on the Performance Prediction Toolkit (PPT), which is
a parallel discrete-event simulator written in Python for rapid assessment and performance prediction of
large-scale scientific applications on supercomputers. PPT is developed by Los Alamos National Laboratory(LANL). The proposed job scheduler simulator incorporates PPT’s application models, 
and when coupled with the sufficiently detailed architecture models, can represent
more realistic job runtime behaviors. Consequently, the simulator can evaluate different job scheduling
and task mapping algorithms on the specific target HPC platforms more accurately.
 

### Dependencies:
 1. Python 2.7 with greenlet
 2. mpi (for parallel runs)


### Running simulation
 1. Navigatge to ppt-sched/multiapp
 2. simply run: python main.py

This will run the a simple Parallels Workloads Archive Trace with defaults. such as FCFS scheduling.
Simply edit in the file the trace you want and job scheduler configurations. 


### Source Organization
 * ppt-sched/multiapp: job scheduler configuration, launch files
 * ppt-sched/software: parallel job scheduling files
 * ppt-sched/apps: some sample parallel PPT applications
 * ppt-sched/hardware: hardware models
 * ppt-sched/middle: MPI model


### ppt-sched/software (Important files)
 * scheduler.py: Scheduling driver driver 
 * algorithms_scheduling.py : Some of the supported sample scheduling algorithm
 * algorithms_mapping.py : task mapping algorithms

### Last but not least
If you need help with anything or have suggestions or would like to contribute please reach me at 
`obaida007 AT gmail.com` or `mobai001 AT fiu.edu`


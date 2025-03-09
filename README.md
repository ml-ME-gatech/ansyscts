# Overview
Some scripts used to create a command line tool, `ansyscts`, for easier management of coupled thermal-fluid simulations and static structural simulations conduced using ANSYS products in a high-performance computing environment using SLURM manager.

## Summary
It is difficult to couple separate simulation processes effectively in a high performance computing environment. The basic scenario for the creation of this library is such:

1. A transient thermal-fluids simulation running in ANSYS Fluent. Temperatures in the whole body are exported from the simulation periodically.

For every periodically written temperature file: 

1. A mapping, using interpolation, of the transient simulation temperatures to the static-structural simulations. 
2. A solve of the static structural simulation.
3. Post-processing of the results.

## Examples

### Basic Usage
```bash
### Runs the command line line tool in the current folder 
ansysact ./ 
```

### Customized Usage
```bash
### Runs the command line tool in sim_folder with 
#1. the (relative to the sim_folder) path to watch as sim_output
#2. debug mode on 
#3. The run-mode set as "continued" meaning that executation of the script will ignore most errors in processing
#4. The maxworkers set to 10 - the maximum number of thread processes dispatching processing of the jobs. 
#5. the queue to submit slurm jobs set to "customqueue".
ansysact sim_folder --path_to_watch sim_output --debug --rmode continued --max_workers 10 --queue customqueue
```


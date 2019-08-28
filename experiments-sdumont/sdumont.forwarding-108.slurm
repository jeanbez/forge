#!/bin/bash
#SBATCH --nodes=68
#SBATCH --ntasks-per-node=1
#SBATCH --ntasks=68
#SBATCH -p nvidia_scal
#SBATCH -J fwd-sim
#SBATCH --exclusive

echo $SLURM_JOB_NODELIST
nodeset -e $SLURM_JOB_NODELIST
cd $SLURM_SUBMIT_DIR

module load openmpi/gnu/4.0.1

export LD_LIBRARY_PATH=/scratch/cenapadrjsd/jean.bez/libconfig-1.7.2/lib/:/scratch/cenapadrjsd/jean.bez/agios/

EXEC=/scratch/cenapadrjsd/jean.bez/forwarding-simulator/fwd-sim sdumont.forwarding-108.json

srun --resv-ports -n $SLURM_NTASKS $EXEC

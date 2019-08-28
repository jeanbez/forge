#!/bin/bash
#SBATCH --nodes=20
#SBATCH -p nvidia_small
#SBATCH -J fwd-sim
#SBATCH --exclusive

echo $SLURM_JOB_NODELIST
cd $SLURM_SUBMIT_DIR

MACHINEFILE="nodes.$SLURM_JOB_ID"
srun -l hostname | sort -n | awk '{print $2}' > $MACHINEFILE

module load openmpi/gnu/4.0.1

export LD_LIBRARY_PATH=/scratch/cenapadrjsd/jean.bez/libconfig-1.7.2/lib/:/scratch/cenapadrjsd/jean.bez/agios/

# Execute the experiment
EXECUTE="/scratch/cenapadrjsd/jean.bez/forwarding-simulator/fwd-sim /scratch/cenapadrjsd/jean.bez/forwarding-simulator/experiments/sdumont.forwarding.test-000.json"

FORWARDERS=4
CLIENTS=16

PER_NODE=(1 2 4 8 16)

for i in "${PER_NODE[@]}"; do 
	PROCESSES=$(($CLIENTS * $i + $FORWARDERS))

	# Generate the hostfile for the given nodes
	echo "python generate.py $MACHINEFILE $SLURM_JOB_NUM_NODES $FORWARDERS $CLIENTS $PROCESSES"

	python generate.py $MACHINEFILE $SLURM_JOB_NUM_NODES $FORWARDERS $CLIENTS $PROCESSES

	echo "mpirun --np $PROCESSES --machinefile hostfile $EXECUTE"

	mpirun --np $PROCESSES --machinefile hostfile $EXECUTE
done
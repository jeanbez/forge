# -*- Coding: UTF-8 -*-
#coding: utf-8

# 
# Usage: python create-experiment.py list.csv
#

import sys
import csv
import os
import os.path
import json

from pprint import pprint

HEADER = 0

#
# Fixed factors
#

FORWARDING_CLIENTS = 64
TOTAL_EXPERIMENT_SIZE = 2 * 1024 * 1024 * 1024 # 2GB

def usage (argv):
	if (len(argv) < 2) or ((len(argv) == 2) and (argv[1] == "-h")):
		print "Usage:\n\tpython create-experiments.py LIST.CSV\n"
		
		exit()

def create_list_from_parameters(parameters):
	new_list = []
	for parameter in parameters:
		if (parameter != '') and (parameter != ' '):
			new_list.append(parameter)
	return new_list

# This function will convert the default factor level -1 or 1 to indexes 0 or 1
def level_to_value(level):
	if int(level) < 0:
		return 0
	return 1


usage(sys.argv)

# Experiment design file
design_file	= sys.argv[1]

#
# Read the desing file
#

design_file_name = os.path.splitext(os.path.basename(design_file))[0]

skipped = 0

experiments = []

index = 0

with open(design_file, 'r') as csvfile:
	designs = csv.reader(csvfile, delimiter=',')

	for i, design in enumerate(designs):
		if i == HEADER:
			continue

		#
		# Parse the parameters
		#

		#   INDEX 	COLUMN
		#   ------------------------------
		#    0		name
		#	 1 		run.no.in.std.order
		#	 2 		run.no
		# 	 3 		run.no.std.rp
		#   ------------------------------
		# 	 4		storage_nodes
		#    5 		forwarders
		#    6 		request
		#    7 		files
		#    8 		spatiality

		# Repetition
		# ['10', '4', '10', '4.2', ... ]
		#                      ^
		repetition = 1 #experiment[3].split('.')[1]

		# Configure the experiment JSON description file
		experiment = {}

		experiment['forwarders'] = str(design[5])
		experiment['listeners'] = '16'
		experiment['dispatchers'] = '16'
		experiment['path'] = '/scratch/cenapadrjsd/jean.bez/lustre-{}-servers-4mb-stripe'.format(design[4])
		experiment['number_of_files'] = str(design[7])
		experiment['spatiality'] = str(design[8])
		experiment['total_size'] = str(TOTAL_EXPERIMENT_SIZE)
		experiment['request_size'] = str(int(design[6]) * 1024 * 1024) # MB

		with open('{}-{:03d}.json'.format(design_file_name, int(design[0])), 'w') as outfile:
			json.dump(experiment, outfile, indent = 4)

		n = int(design[4]) + FORWARDING_CLIENTS

		# Configure the experiment slum description file
		slurm = (
			'#!/bin/bash\n'
			'#SBATCH --nodes=' + str(n) + '\n'	# Número de Nós
			'#SBATCH --ntasks-per-node=1\n'		# Número de tarefas por Nó
			'#SBATCH --ntasks=' + str(n) + '\n'	# Número total de tarefas MPI
			'#SBATCH -p nvidia_scal\n'			# Fila (partition) a ser utilizada
			'#SBATCH -J fwd-sim\n'				# Nome job
			'#SBATCH --exclusive\n'				# Utilização exclusiva dos nós durante a execução do job
			'\n'
			'echo $SLURM_JOB_NODELIST\n'		# Exibe os nós alocados para o Job	
			'nodeset -e $SLURM_JOB_NODELIST\n'	
			'cd $SLURM_SUBMIT_DIR\n'
			'\n'
			'module load openmpi/gnu/4.0.1\n'	# Configura os compiladores
			'\n'
			'export LD_LIBRARY_PATH=/scratch/cenapadrjsd/jean.bez/libconfig-1.7.2/lib/:/scratch/cenapadrjsd/jean.bez/agios/\n'
			'\n'
			'EXEC=/scratch/cenapadrjsd/jean.bez/forwarding-simulator/fwd-sim ' + '{}-{:03d}.json'.format(design_file_name, int(design[0])) + '\n'
			'\n'
			'srun --resv-ports -n $SLURM_NTASKS $EXEC\n'
		)

		with open('{}-{:03d}.slurm'.format(design_file_name, int(design[0])), 'w') as outfile:
			outfile.write(slurm)





		
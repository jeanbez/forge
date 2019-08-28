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

FORWARDING_CLIENTS = 32
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
		#    4 		forwarders
		#    5 		request
		#    6 		files
		#    7 		spatiality
                #    8          processes_per_node

		# Repetition
		# ['10', '4', '10', '4.2', ... ]
		#                      ^
		repetition = design[3].split('.')[1]

		# Configure the experiment JSON description file
		experiment = {}

		if (design[6] == 'individual' and design[7] == 'strided'):
			continue

		experiment['forwarders'] = str(design[4])
		experiment['listeners'] = '16'
		experiment['dispatchers'] = '16'
		experiment['path'] = '/mnt/orangefs/test-{}'.format(index)
		experiment['number_of_files'] = str(design[6])
		experiment['spatiality'] = str(design[7])
		experiment['total_size'] = str(TOTAL_EXPERIMENT_SIZE)
		experiment['request_size'] = str(design[5])

		with open('{}-{:04d}.json'.format(design_file_name, index), 'w') as outfile:
			json.dump(experiment, outfile, indent = 4)

		n = int(experiment['forwarders']) + (int(FORWARDING_CLIENTS) * int(design[8]))

		bash = (
			'#!/bin/bash\n'
			'\n'
			'mpirun --allow-run-as-root --mca io romio314 --mca btl ^openib --np ' + str(n) + ' --hostfile /root/hostfile /root/forwarding-simulator/fwd-sim ' + '/root/experiments-grid-5000/{}-{:04d}.json'.format(design_file_name, index) + '\n'
		)

		with open('{}-{:04d}.bash'.format(design_file_name, index), 'w') as outfile:
			outfile.write(bash)

		index = index + 1

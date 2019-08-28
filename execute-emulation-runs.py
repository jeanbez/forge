import os
import re
import sys
import commands
import time
import glob
import json
import subprocess

from datetime import datetime
from datetime import timedelta

from pprint import pprint

now = datetime.now()

FRONTEND = 'jbez@nancy:~/mission/%d-%d-%d/' % (now.year, now.month, now.day)

CLIENTS = 32

# Make sure the basefile exists

if not os.path.isfile('nodes'):
	print '> unable to find "nodes" file'

	exit()

if not os.path.isfile('iofsl-servers'):
	print '> unable to find "iofsl-servers" file'

	exit()

if not os.path.isfile('iofsl-clients'):
	print '> unable to find "iofsl-clients" file'

	exit()

# Open the nodes file
with open('nodes') as f:
	nodes = f.read().splitlines()

print '> synchronize forwarding-simulator...'

# Synchronize the forwarding code with all the nodes
for node in nodes:
	os.system("rsync -r /root/forwarding-simulator root@{0}:~/".format(node))

print '> sending AGIOS files...'

# Copy the AGIOS file to servers
for node in nodes:
	os.system("rsync /root/forwarding-simulator/agios/* root@{0}:/tmp/".format(node))

# Get all the .bash files
emulations = glob.glob('*.bash')

# Loop through the experiments
for e, emulation in enumerate(emulations):
	
	processes = None
	forwarders = None

	# Get the label of the experiment
	label = os.path.basename(emulation).replace('.bash', '')

	print '> emulation: {0}'.format(label)

	# Get the JSON configuration file
	configuration = emulation.replace('bash', 'json')

	print '> configuration: {0}'.format(configuration)
	
	# Read the file and parse the JSON
	with open(configuration) as json_file:
		try:
			configuration_json = json.load(json_file)

			FORWARDERS = int(configuration_json['forwarders'])

			print '> forwarders: {0}'.format(FORWARDERS)
		except Exception as e:
			print '> {0}'.format(e)

			continue

	# Get the number of forwarders and processes
	with open(emulation, 'r') as bash_file:
		configuration_bash = bash_file.read().splitlines()
		
		for line in configuration_bash:
			if 'mpi' in line:
				matches = re.search(r"--np (\d*)", line)
				
				if matches:
					PROCESSES = int(matches.groups()[0])

					print '> processes: {0}'.format(PROCESSES)

	if not PROCESSES or not FORWARDERS:
		print 'skipping emulation due to misconfiguration'

		continue
	
	# Create directory to store the logs
	logs = 'logs-{0}'.format(label)

	if os.path.isdir(logs):
		print '> directory "{0}" already exists'.format(logs)
	else:
		print '> creating {0} to store results'.format(logs)
		os.system('mkdir {0}'.format(logs))
	
	# Generate the hosfile
	output = open('hostfile', 'w')

	f = open('iofsl-servers', 'r')
	c = open('iofsl-clients', 'r')

	forwarders = f.readlines()
	clients = c.readlines()

	f.close()
	c.close()

	# Split the selected nodes into the two sets
	f_nodes = forwarders[:FORWARDERS]
	c_nodes = clients[:CLIENTS]

	for node in f_nodes:
	    output.write('{0}:{1}\n'.format(node.strip(), 1))

	slots = int((PROCESSES - FORWARDERS) / CLIENTS)

	for node in c_nodes:
	    output.write('{0}:{1}\n'.format(node.strip(), slots))

	output.close()

	print '> store the hostfile'
	os.system('cp hostfile logs-{0}/'.format(label))

	print '> sending files to compute nodes...'

	for node in nodes:
		# Send the updated bash and JSON files to all nodes
		os.system('rsync {1} root@{0}:~/{1}'.format(node, configuration))
		os.system('rsync {1} root@{0}:~/{1}'.format(node, emulation))

		# Send the hostfile to all nodes
		os.system('rsync hostfile root@{0}:~/hostfile'.format(node))

	print '> running...'

	# Run the emulation
	os.system('bash {0}'.format(emulation))

	print '> done!'

	# Get the output files

	print '> retrieving map results...'
	
	# The timing information in stored by the master in rank 0
	os.system('scp {0}:~/*.map logs-{1}/'.format(f_nodes[0].strip(), label))

	print '> retrieving time results...'
	
	# The timing information in stored by the master in rank 0
	os.system('scp {0}:~/*.time logs-{1}/'.format(c_nodes[0].strip(), label))

	print '> retrieving statistics results...'

	# The statistics information about each forwarding node is stored in each forwarding node
	for node in f_nodes:
		# Copy the statistics file
		
		os.system('scp {0}:~/*.stats logs-{1}/'.format(node.strip(), label))

	print '> store the emulator.log'

	# Save the emulation log file
	os.system('scp {0}:~/emulator.log logs-{1}/'.format(f_nodes[0].strip(), label))

	# Remove the emulation log file
	os.system('ssh {0} rm emulator.log'.format(f_nodes[0].strip()))

	print '> remove the files in the file system'

	# Remove the PVFS file
	os.system('/opt/pvfs/bin/pvfs2-rm {0}'.format(configuration_json['path']))

	for node in nodes:
		# Remove the local files in the all the nodes
		os.system('ssh {0} rm -f hostfile'.format(node))

	print '> ------------------------------------------------------------------------------'
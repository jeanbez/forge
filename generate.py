import sys
import json


bashfile = sys.argv[1]

b = open(bashfile, 'r')

lines = b.readlines()

P = None
F = None
C = 32

for line in lines:
    if '--np' in line:
        P = int(line.split(' ')[9])

b.close()

jsonfile = bashfile.replace('bash', 'json')

j = open(jsonfile, 'r')
config = json.load(j)

F = int(config['forwarders'])

j.close()

output = open('hostfile', 'w')

f = open('nodes-forwarding', 'r')
c = open('nodes-clients', 'r')

forwarders = f.readlines()
clients = c.readlines()

f.close()
c.close()

# Split the selected nodes into the two sets
f_nodes = forwarders[:F]
c_nodes = clients[:C]

for node in f_nodes:
    output.write('{} slots={}\n'.format(node.strip(), 1))

slots = int((P - F) / C)

for node in c_nodes:
    output.write('{} slots={}\n'.format(node.strip(), slots))

output.close()

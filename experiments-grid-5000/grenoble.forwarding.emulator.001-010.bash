#!/bin/bash

mpirun --allow-run-as-root --mca io romio314 --mca btl ^openib --np 28 --hostfile /root/nodes-clients /root/forwarding-simulator/fwd-sim /root/experiments-grid-5000/grenoble.forwarding.emulator.001-010.json

#!/bin/bash

mpirun --allow-run-as-root --mca io romio314 --mca btl ^openib --np 68 --hostfile /root/hostfile /root/forwarding-simulator/fwd-sim /root/experiments-grid-5000/forwarding.emulator.001-0847.json

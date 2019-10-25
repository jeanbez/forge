#!/bin/bash

git clone https://gitlab.com/jeanbez/agios.git
cd agios
make clean
make library
make library_install
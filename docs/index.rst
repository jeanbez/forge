.. FORGE documentation master file, created by
   sphinx-quickstart on Wed Oct 30 16:50:27 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::

FORGE
=====

The goal of the I/O FORwardinG Emulator, a.k.a., **FORGE** is to quickly evaluate new I/O optimizations (such as new request schedulers) and modifications on I/O forwarding deployment and configuration on large-scale clusters and supercomputers. As modification on production-scale machines are often not allowed (as it could disrupt services), this straightforward emulator seeks to be a research alternative. 

Installation
------------

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

Prerequisites
^^^^^^^^^^^^^

The emulator needs the AGIOS scheduling library for the I/O nodes to have request scheduling capabilities. To install AGIOS:

.. code-block:: bash

   git clone https://bitbucket.org/francielizanon/agios
   cd agios
   make library
   make library_install

You must also have the GSL - GNU Scientific Library installed. To install GSL:

.. code-block:: bash

   apt install libgsl-dev

Building
^^^^^^^^

Building the forwarding emulator is straightforward:

.. code-block:: bash

   git clone https://gitlab.com/jeanbez/forwarding-emulator
   cd forwarding-emulator
   mkdir build
   cd build
   cmake ..
   make

If you want FORGE to output debug messages (very verbose and should be used for development only), replace the cmake line by:

.. code-block:: bash

   cmake -DDEBUG=ON ..

Emulating
---------

You first need to configure the AGIOS scheduling library and then the scenario (setup and configuration) you want to emulate.

Setup AGIOS
^^^^^^^^^^^

You need to copy some files to /tmp on each node AGIOS will run. These files are required as they contain configuration parameters and access times. More information about these files, please refer to the AGIOS repository and paper.

.. code-block:: bash

   cd forwarding-emulator
   cp agios/* /tmp/

Emulator Configuration
^^^^^^^^^^^^^^^^^^^^^^

The emulator is capable of mock different access patterns (based on MPI-IO Test Benchmark). It takes several parameters to configure the forwarding nodes.

The Emulator
============

I/O Requests
------------

.. doxygenstruct:: request
   :project: forge
   :members:

.. doxygenstruct:: forwarding_request
   :project: forge
   :members:

AGIOS
-----

AGIOS scheduling library requires one callback function to inform that requests are ready to be processed. If has also support for using an aggregated call when multiple requests are ready to be processed.

.. doxygenfunction:: callback
   :project: forge

.. doxygenfunction:: callback_aggregated
   :project: forge

Forwarding Nodes
----------------

Each I/O forwarding node spans multiple threads to handle incoming messages, requests, and dispatch them to the file system. The main three types of threads are: listener, handler, and dispatcher.

.. doxygenfunction:: server_listener
   :project: forge

.. doxygenfunction:: server_handler
   :project: forge

.. doxygenfunction:: server_dispatcher
   :project: forge

Statistics
----------

.. doxygenstruct:: forwarding_statistics
   :project: forge
   :members:

Acknowledgments
===============

This study was financed in part by the Coordenação de Aperfeiçoamento de Pessoal de Nível Superior - Brasil (CAPES) - Finance Code 001. It has also received support from the Conselho Nacional de Desenvolvimento Científico e Tecnológico (CNPq), Brazil.
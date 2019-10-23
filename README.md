![FORGE](forge.png)

# I/O Forwarding Emulator

The goal of the I/O **FOR**wardin**G E**mulator, a.k.a., **FORGE** is to quickly evaluate new I/O optimizations (such as new request schedulers) and modifications on I/O forwarding deployment and configuration on large-scale clusters and supercomputers. As modification on production-scale machines are often not allowed (as it could disrupt services), this straightforward emulator seeks to be a research alternative. 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

The emulator needs the AGIOS scheduling library for the I/O nodes to have request scheduling capabilities. To install AGIOS:

```
git clone https://bitbucket.org/francielizanon/agios
cd agios
make library
make library_install
```

You must also have the GSL - GNU Scientific Library installed. To install GSL:

```
apt install libgsl-dev
```

### Building

Building the forwarding emulator is straightforward:

```
git clone https://gitlab.com/jeanbez/forwarding-emulator
cd forwarding-emulator
mkdir build
cd build
cmake ..
make
```

If you want FORGE to output debug messages (very verbose and should be used for development only), replace the `cmake` line by:

```
cmake -DDEBUG=ON ..
```

## Emulating

You first need to configure the AGIOS scheduling library and then the scenario (setup and configuration) you want to emulate.

### Setup AGIOS

You need to copy some files to `/tmp` on each node AGIOS will run. These files are required as they contain configuration parameters and access times. More information about these files, please refer to the AGIOS repository and paper.

```
cd forwarding-emulator
cp agios/* /tmp/
```

### Emulator Configuration

The emulator is capable of mock different access patterns (based on MPI-IO Test Benchmark). It takes several parameters to configure the forwarding nodes.

| Parameter | Description |
| -------------- | -------------- |
| `forwarders` | Number of the first `N` MPI processes that will act as I/O forwarding servers. |
| `handlers` | Number of threads to handle incoming messages from the compute nodes connected to the forwarding node. |
| `dispatchers` | Number of threads to issue requests to the file system. |
| `path` | The absolute path to where the file should be written. In case of a file per process, this path will act as a prefix for the final filename. |
| `number_of_files` | The number of files that the emulator will use. It supports two values: `individual` and `shared`. In the first, each process will write/read to its own independent file, whereas for the latter, all processes will share the same file. |
| `spatiality` | Defines the spatiality of the accesses. It supports `contiguous` and `strided` accesses. Notice that the `strided` option *cannot* be used with the `individual` file layout. |
| `total_size` | Defines the total size of the simulation (in bytes). |
| `request_size` | Defines the size of each request (in bytes). |
| `validation` | Validate each byte read by the emulator. This option **will** interfere with the total execution time. Therefore, please do not use it while collecting runtime. |

The emulator receives a JSON file with the parameters for the execution. An example of a configuration file is presented below. 

```
{
    "forwarders": "1",
    "handlers": "16",
    "dispatchers": "16",
    "path": "/mnt/pfs/test",
    "number_of_files": "shared",
    "spatiality": "contiguous",
    "total_size": "4294967296",
    "request_size": "32768",
    "validation": "1"
}
```

Furthermore, the emulator expects a `hostfile` with proper mapping of processes per compute node. Since the first `N` MPI processes will act as forwarders, you need to ensure that the first `N` nodes in the list have a single slot available. 

```
grisou-1.nancy.grid5000.fr:1
grisou-10.nancy.grid5000.fr:4
grisou-11.nancy.grid5000.fr:4
grisou-12.nancy.grid5000.fr:4
grisou-13.nancy.grid5000.fr:4
grisou-14.nancy.grid5000.fr:4
grisou-16.nancy.grid5000.fr:4
grisou-19.nancy.grid5000.fr:4
grisou-20.nancy.grid5000.fr:4
```

Once you have the configuration file prepared, you can launch the emulator. However, notice that you need to start additional `forwarders` MPI processes. For instance, if you want to emulate 128 clients and 4 forwarders, you need to use `--op 132`. The first `forwarders` MPI processes will be placed in separate nodes (one per node if you `hostfile` was correctly defined). The remainder of the process will be allocated to other compute nodes.

## Statistics

The emulator will generate a couple of files. The `.map` file will detailed the mapping of the processes (forwarding servers and clients).

```
rank 0: server
rank 1: client
rank 2: client
rank 3: client
rank 4: client
rank 5: client
rank 6: client
rank 7: client
rank 8: client
rank 9: client
```

One `.stat` file will be generated for each I/O forwarding server containing the number of open, read, write, and close operations handled by that server. Furthermore, it also presents the total write and read size.

```
forwarder: 0
open: 8
read: 500
write: 500
close: 8
read_size: 5000
write_size: 5000
```

Finally, the execution time of the write and read phases is detailed in the `*.time` file. It also presents the time taken by each process (to allow the detection of stragglers) and the minimum, maximum, median, and average execution time of all the processes. If you are interested in the execution time as perceived by the user, i.e., makespan time, you should use the *maximum* value.

```
---------------------------
 I/O Forwarding Simulation
---------------------------
 | 2019-10-23 | 16:45:31 | 
---------------------------
 forwarders:             2
 clients:                8
 layout:                 1
 spatiality:             1
 request:               10
 total:              10000
---------------------------

 WRITE
---------------------------
 rank 000:       5.3445046
 rank 001:       5.0604975
 rank 002:       5.3449700
 rank 003:       5.3150820
 rank 004:       5.0331093
 rank 005:       4.7507457
 rank 006:       4.9803173
 rank 007:       4.8717232
---------------------------
      min:       4.7507457
       Q1:       4.9531688
       Q2:       5.0468034
       Q3:       5.3224377
      max:       5.3449700
     mean:       5.0876187
---------------------------

 READ
---------------------------
 rank 000:       5.3355468
 rank 001:       5.3359355
 rank 002:       5.2250329
 rank 003:       5.3348376
 rank 004:       4.9645267
 rank 005:       4.9469373
 rank 006:       4.9617581
 rank 007:       5.0456340
---------------------------
      min:       4.9469373
       Q1:       4.9638345
       Q2:       5.1353335
       Q3:       5.3350149
      max:       5.3359355
     mean:       5.1437761
---------------------------
    mean:       1.9217235
---------------------------
```

## Acknowledgments

This study was financed in part by the Coordenação de Aperfeiçoamento de Pessoal de Nível Superior - Brasil (CAPES) - Finance Code 001. It has also received support from the Conselho Nacional de Desenvolvimento Científico e Tecnológico (CNPq), Brazil.
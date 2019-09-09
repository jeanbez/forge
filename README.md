# I/O Forwarding Emulator

The goal of this project is to quickly evaluate new I/O optimizations (such as new request schedulers) and modifications on I/O forwarding deployment and configuration on large-scale clusters and supercomputers. As modification on production-scale machines are often not allowed (as it could disrupt services), this straightforward emulator seeks to be a research alternative. 

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

### Building

Building the forwarding emulator is straightforward:

```
git clone https://gitlab.com/jeanbez/forwarding-emulator
make
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
| `listeners` | Number of threads listening to incoming messages from the compute nodes connected to the forwarding node. |
| `dispatchers` | Number of threads to issue requests to the file system. |
| `path` | The absolute path to where the file should be written. In case of a file per process, this path will act as a prefix for the final filename. |
| `number_of_files` | The number of files that the emulator will use. It supports two values: `individual` and `shared`. In the first, each process will write/read to its own independent file, whereas for the latter, all processes will share the same file. |
| `spatiality` | Defines the spatiality of the accesses. It supports `contiguous` and `strided` accesses. Notice that the `strided` option *cannot* be used with the `individual` file layout. |
| `total_size` | Defines the total size of the simulation (in bytes). |
| `request_size` | Defines the size of each request (in bytes). |
| `validation` | Validate each byte read by the emulator. This option *will* interfere with the total execution time. Therefore, please do not use it while collecting runtime. |

The emulator receives a JSON file with the parameters for the execution. An example of a configuration file is presented below. 

```
{
    "forwarders": "1",
    "listeners": "16",
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

## Acknowledgments

This study was financed in part by the Coordenação de Aperfeiçoamento de Pessoal de Nível Superior - Brasil (CAPES) - Finance Code 001. It has also received support from the Conselho Nacional de Desenvolvimento Científico e Tecnológico (CNPq), Brazil.
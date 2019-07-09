#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <mpi.h>
#include <fcntl.h>
#include <math.h>
#include <unistd.h>
#include <agios.h>
#include <limits.h>
#include <time.h>

// sudo apt-get install libgsl-dev
#include <gsl/gsl_sort.h>
#include <gsl/gsl_statistics.h>

#include "jsmn.h"
#include "fwd_list.h"
#include "uthash.h"

#define READ 0
#define WRITE 1
#define OPEN 3
#define CLOSE 4

#define MAXIMUM_REQUEST_SIZE (1* 1024 * 1024 * 1024)

#define FWD_MAX_LISTEN_THREADS 128
#define FWD_MAX_PROCESS_THREADS 128

#define TAG_REQUEST 10001
#define TAG_BUFFER 10002
#define TAG_ACK 10003
#define TAG_HANDLE 10004

#define INDIVIDUAL 0
#define SHARED 1

#define CONTIGUOUS 0
#define STRIDED 1

#define AGIOS_CONFIGURATION "/tmp/agios.conf"
/*#define AGIOS_CONFIGURATION "/scratch/cenapadrjsd/jean.bez/agios/agios.conf"*/

#define ERROR_FAILED_TO_PARSE_JSON 70001
#define ERROR_INVALID_JSON 70002
#define ERROR_AGIOS_REQUEST 70003
#define ERROR_SEEK_FAILED 70004
#define ERROR_WRITE_FAILED 70005
#define ERROR_READ_FAILED 70006
#define ERROR_INVALID_REQUEST_ID 70007
#define ERROR_INVALID_PATTERN 70008
#define ERROR_INVALID_SETUP 70009
#define ERROR_MEMORY_ALLOCATION 70010
#define ERROR_UNSUPPORTED 70011
#define ERROR_UNKNOWN_REQUEST_TYPE 70012
#define ERROR_INVALID_FILE_HANDLE 70013
#define ERROR_FAILED_TO_CLOSE 70014
#define ERROR_INVALID_VALIDATION 70015

struct request {
	char file_name[255];
	int file_handle;

	int operation;

	unsigned long offset;
	unsigned long size;
};

// Structure to keep track of the requests in the forwarding layer
struct forwarding_request {
	unsigned long id;

	int rank;

	char file_name[255];
	int file_handle;
	
	int operation;
	unsigned long offset;
	unsigned long size;
	char *buffer;

	UT_hash_handle hh;
};

struct ready_request {
	int id;

	struct fwd_list_head list;
};

// Struture to keep track of open file handles
struct opened_handles {
	int fh;

	char path[255];
	int references;

	UT_hash_handle hh;
};

// Structure to store statistics of requests in each forwarding
struct forwarding_statistics {
	unsigned long int open;
	unsigned long int read;
	unsigned long int write;
	unsigned long int close;

	unsigned long int read_size;
	unsigned long int write_size;
};

unsigned long generate_identifier();

void callback(unsigned long long int id);

void start_AGIOS();
void stop_AGIOS();

int get_forwarding_server();

void *server_listen(void *p);
void *server_dispatcher(void *p);

void safe_memory_free(void ** pointer_address, char * id);
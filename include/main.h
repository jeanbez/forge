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

#include <gsl/gsl_sort.h>
#include <gsl/gsl_statistics.h>

#include "jsmn.h"
#include "pqueue.h"

#define ERROR_FAILED_TO_PARSE_JSON 70001 	/*!< Failed to parse the JSON file. */
#define ERROR_INVALID_JSON 70002			/*!< Invalid JSON file. */
#define ERROR_AGIOS_REQUEST 70003			/*!< Error when sending a request to AGIOS. */
#define ERROR_SEEK_FAILED 70004				/*!< Error when seeking a position in file. */
#define ERROR_WRITE_FAILED 70005			/*!< Error when issuing a write operation. */
#define ERROR_READ_FAILED 70006				/*!< Error when issuing a read operation. */
#define ERROR_INVALID_REQUEST_ID 70007		/*!< Invalid request ID code. */
#define ERROR_INVALID_PATTERN 70008			/*!< Invalid access pattern to emulate. */
#define ERROR_INVALID_SETUP 70009			/*!< Invalid emulation setup. */
#define ERROR_MEMORY_ALLOCATION 70010		/*!< Unable to allocate the necessary memory. */
#define ERROR_UNSUPPORTED 70011				/*!< Unsupported operation. */
#define ERROR_UNKNOWN_REQUEST_TYPE 70012	/*!< Unkown request type. */
#define ERROR_INVALID_FILE_HANDLE 70013		/*!< Invalid file handle. */
#define ERROR_FAILED_TO_CLOSE 70014			/*!< Failed to close a file. */
#define ERROR_INVALID_VALIDATION 70015		/*!< Invalid validation option was provided by the user. */
#define ERROR_POSIX_OPEN 700016				/*!< Error when operning a file using POSIX. */
#define ERROR_PVFS_OPEN 700017				/*!< Error when operning a file using PVFS. */
#define ERROR_VALIDATION_FAILED 700018		/*!< Validation failed. */
#define ERROR_AGIOS_INITIALIZATION 700019   /*!< Unable to initialize AGIOS, check for the configuration file. */

/**
 * A structure to hold a client request.
 */ 
struct request {
	char file_name[255];			/*!< File name required for open operations. */
	int file_handle;				/*!< Once the file has been open, store the handle for future operations. */

	int operation;					/*!< Identify the I/O operation. */

	unsigned long offset;			/*!< Offset of the request in the file. */
	unsigned long size;				/*!< Size of the request. */
};

unsigned long long int generate_identifier();

void callback(unsigned long long int id);
void callback_aggregated(unsigned long long int *ids, int total);

void start_AGIOS();
void stop_AGIOS();

int get_forwarding_server();

void *server_listener(void *p);
void *server_handler(void *p);
void *server_dispatcher(void *p);

void safe_memory_free(void ** pointer_address, char * id);

typedef struct node_t {
	pqueue_pri_t priority;
	int value;
	size_t position;
} node_t;

static int compare_priority(pqueue_pri_t next, pqueue_pri_t current) {
	return (next < current);
}

static pqueue_pri_t get_priority(void *a) {
	return ((node_t *) a)->priority;
}

static void set_priority(void *a, pqueue_pri_t priority) {
	((node_t *) a)->priority = priority;
}

static size_t get_position(void *a) {
	return ((node_t *) a)->position;
}

static void set_position(void *a, size_t position) {
	((node_t *) a)->position = position;
}

#ifdef DEBUG
static void print_node(FILE *out, void *a) {
	node_t *n = a;

	fprintf(out, "priority: %lld, value: %d\n", n->priority, n->value);
}
#endif
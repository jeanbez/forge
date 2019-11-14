#pragma once

#include "fwd_list.h"
#include "thpool.h"
#include "uthash.h"

#ifdef PVFS
#include "pvfs2.h"
#include "dispatcher/orangefs-types.h"
#endif

#define ERROR_FAILED_TO_PARSE_JSON 70001 			/*!< Failed to parse the JSON file. */
#define ERROR_INVALID_JSON 70002					/*!< Invalid JSON file. */
#define ERROR_AGIOS_REQUEST 70003					/*!< Error when sending a request to AGIOS. */
#define ERROR_SEEK_FAILED 70004						/*!< Error when seeking a position in file. */
#define ERROR_WRITE_FAILED 70005					/*!< Error when issuing a write operation. */
#define ERROR_READ_FAILED 70006						/*!< Error when issuing a read operation. */
#define ERROR_INVALID_REQUEST_ID 70007				/*!< Invalid request ID code. */
#define ERROR_INVALID_PATTERN 70008					/*!< Invalid access pattern to emulate. */
#define ERROR_INVALID_SETUP 70009					/*!< Invalid emulation setup. */
#define ERROR_MEMORY_ALLOCATION 70010				/*!< Unable to allocate the necessary memory. */
#define ERROR_UNSUPPORTED 70011						/*!< Unsupported operation. */
#define ERROR_UNKNOWN_REQUEST_TYPE 70012			/*!< Unkown request type. */
#define ERROR_INVALID_FILE_HANDLE 70013				/*!< Invalid file handle. */
#define ERROR_FAILED_TO_CLOSE 70014					/*!< Failed to close a file. */
#define ERROR_INVALID_VALIDATION 70015				/*!< Invalid validation option was provided by the user. */
#define ERROR_POSIX_OPEN 700016						/*!< Error when operning a file using POSIX. */
#define ERROR_PVFS_OPEN 700017						/*!< Error when operning a file using PVFS. */
#define ERROR_VALIDATION_FAILED 700018				/*!< Validation failed. */
#define ERROR_AGIOS_INITIALIZATION 700019 			/*!< Unable to initialize AGIOS, check for the configuration file. */
#define ERROR_FAILED_TO_LOAD_JSON 700020		 	/*!< Unable to read the JSON file. */

#define READ 0										/*!< Identify read operations. */
#define WRITE 1										/*!< Identify write operations. */
#define OPEN 3										/*!< Identify open operations. */
#define CLOSE 4										/*!< Identify close operations. */

#define MAX_REQUEST_SIZE (128 * 1024 * 1024)		/*!< Maximum allowed request size to emulate. */
#define MAX_BUFFER_SIZE (1 * 1024 * 1024 * 1024)	/*!< Maximum allowed buffer size for aggregated requests. */
#define MAX_BATCH_SIZE 16							/*!< Maximum number of contiguous requests that should be merged together before issuing the request. */
#define MAX_QUEUE_ELEMENTS 1024						/*!< Maximum nuber of requests in the queue. */

#define FWD_MAX_HANDLER_THREADS 128					/*!< Maximum number of threads to handle the incoming requests. */
#define FWD_MAX_PROCESS_THREADS 128					/*!< Maximum number of threads to issue and process the requests. */

#define TAG_REQUEST 10001							/*!< MPI tag to identify requests. */
#define TAG_BUFFER 10002							/*!< MPI tag to identify buffers. */
#define TAG_ACK 10003								/*!< MPI tag to identify acknownledgment. */
#define TAG_HANDLE 10004							/*!< MPI tag to identify a file handle. */

#define INDIVIDUAL 0								/*!< Indicate access to individual files, i.e. file per process. */
#define SHARED 1									/*!< Indicate access to shared files. */

#define CONTIGUOUS 0								/*!< Issue contiguous requests. */
#define STRIDED 1									/*!< Issue requests following a 1D-strided spatiality. */

#define TIMEOUT 1									/*!< Timeout (in seconds). */

#define AGIOS_CONFIGURATION "/tmp/agios.conf"		/*<! Path to the AGIOS configuration file. */

/**
 * A structure to hold a client request.
 */ 
struct request {
	char file_name[255];							/*!< File name required for open operations. */
	int file_handle;								/*!< Once the file has been open, store the handle for future operations. */

	int operation;									/*!< Identify the I/O operation. */

	unsigned long offset;							/*!< Offset of the request in the file. */
	unsigned long size;								/*!< Size of the request. */
};

/**
 * A structure to hold an I/O forwarding request.
 */ 
struct forwarding_request {
	/**
	 * @name Request identification.
	 */
	/*@{*/
	unsigned long long int id;						/*!< Request identification. */
	int rank;										/*!< Client identification. */
	/*@}*/

	/**
	 * @name I/O request information.
	 */
	/*@{*/
	char file_name[255];							/*!< File name required for open operations. */
	int file_handle;								/*!< Once the file has been open, store the handle for future operations. */
	int operation;									/*!< Identify the I/O operation. */
	unsigned long offset;							/*!< Offset of the request in the file. */
	unsigned long size;								/*!< Size of the request. */
	char *buffer;									/*!< Temporary buffer to store the content of the request. */
	/*@}*/

	struct fwd_list_head list;						/*!< To handle the request while in the incoming queue. */
	UT_hash_handle hh;								/*!< To handle the request once it is scheduled by AGIOS. */
};

/**
 * A structure to hold a request ready to be issued to the file system.
 */ 
struct ready_request {
	unsigned long long int id;						/*!< Request identification. */

	struct fwd_list_head list;						/*!< To handle the request while in the ready queue. */
};

struct aggregated_request {
	int count;										/*!< The number of aggregated requests. */
	int size;										/*!< The aggregated size of the requests. */
	struct forwarding_request *r;   				/*!< The baseline forwarding request, i.e., the first one. */

	unsigned long long int ids[MAX_BATCH_SIZE];		/*!< A list with all the requests ID that compose this aggregated request. */

	char *buffer;									/*!< The aggreated buffer used by this request. */
};

// Struture to keep track of open file handles
struct opened_handles {
	int fh;											/*!< File handle. */

	char path[255];									/*!< Complete path of the given file handle. */
	int references;									/*!< Counter for the number of references. */
	#ifdef PVFS
	pvfs2_file_object pvfs_file;					/*!< PVFS file object to access the file. */
	#endif

	UT_hash_handle hh;								/*!< To handle the file. */
	UT_hash_handle hh_pvfs;							/*!< To handle the file in PVFS. */
};

#ifdef STATISTICS
// Structure to store statistics of requests in each forwarding
struct forwarding_statistics {
	unsigned long int open;							/*!< Number of open operations. */
	unsigned long int read;							/*!< Number of read operations. */
	unsigned long int write;						/*!< Number of write operations. */
	unsigned long int close;						/*!< Number of close operations. */

	unsigned long int read_size;					/*!< Total size of read requests. */
	unsigned long int write_size;					/*!< Total size of write requests. */
};
#endif

pthread_rwlock_t requests_rwlock;
pthread_rwlock_t handles_rwlock;

// Declares the hash to hold the requests and initialize it to NULL (mandatory to initialize to NULL)
struct forwarding_request *requests;
struct opened_handles *opened_files;
#ifdef PVFS
struct opened_handles *opened_pvfs_files;
#endif

pthread_mutex_t incoming_queue_mutex;
pthread_cond_t incoming_queue_signal;

pthread_mutex_t ready_queue_mutex;
pthread_cond_t ready_queue_signal;

threadpool thread_pool;

struct fwd_list_head incoming_queue;
struct fwd_list_head ready_queue;

// Controle the shutdown signal
int shutdown;
int shutdown_control;
pthread_mutex_t shutdown_lock;

int simulation_forwarders;
int world_size, world_rank;

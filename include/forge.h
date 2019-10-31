#pragma once

#include "fwd_list.h"
#include "uthash.h"

#define READ 0									/*!< Identify read operations. */
#define WRITE 1									/*!< Identify write operations. */
#define OPEN 3									/*!< Identify open operations. */
#define CLOSE 4									/*!< Identify close operations. */

#define MAX_REQUEST_SIZE (128 * 1024 * 1024)
#define MAX_AGGREGATED_BUFFER_SIZE (1 * 1024 * 1024 * 1024)
#define MAX_BATCH_SIZE 16						/*!< Maximum number of contiguous requests that should be merged together before issuing the request. */
#define MAX_QUEUE_ELEMENTS 1024					/*!< Maximum nuber of requests in the queue. */

#define FWD_MAX_HANDLER_THREADS 128				/*!< Maximum number of threads to handle the incoming requests. */
#define FWD_MAX_PROCESS_THREADS 128				/*!< Maximum number of threads to issue and process the requests. */

#define TAG_REQUEST 10001						/*!< MPI tag to identify requests. */
#define TAG_BUFFER 10002						/*!< MPI tag to identify buffers. */
#define TAG_ACK 10003							/*!< MPI tag to identify acknownledgment. */
#define TAG_HANDLE 10004						/*!< MPI tag to identify a file handle. */

#define INDIVIDUAL 0							/*!< Indicate access to individual files, i.e. file per process. */
#define SHARED 1								/*!< Indicate access to shared files. */

#define CONTIGUOUS 0							/*!< Issue contiguous requests. */
#define STRIDED 1								/*!< Issue requests following a 1D-strided spatiality. */

#define TIMEOUT 1								/*!< Timeout (in seconds). */

#define AGIOS_CONFIGURATION "/tmp/agios.conf"	/*<! Path to the AGIOS configuration file. */

/**
 * A structure to hold an I/O forwarding request.
 */ 
struct forwarding_request {
	/**
	 * @name Request identification.
	 */
	/*@{*/
	unsigned long long int id;		/*!< Request identification. */
	int rank;						/*!< Client identification. */
	/*@}*/

	/**
	 * @name I/O request information.
	 */
	/*@{*/
	char file_name[255];			/*!< File name required for open operations. */
	int file_handle;				/*!< Once the file has been open, store the handle for future operations. */
	int operation;					/*!< Identify the I/O operation. */
	unsigned long offset;			/*!< Offset of the request in the file. */
	unsigned long size;				/*!< Size of the request. */
	char *buffer;					/*!< Temporary buffer to store the content of the request. */
	/*@}*/

	struct fwd_list_head list;		/*!< To handle the request while in the incoming queue. */
	UT_hash_handle hh;				/*!< To handle the request once it is scheduled by AGIOS. */
};

/**
 * A structure to hold a request ready to be issued to the file system.
 */ 
struct ready_request {
	unsigned long long int id;		/*!< Request identification. */

	struct fwd_list_head list;		/*!< To handle the request while in the ready queue. */
};

// Struture to keep track of open file handles
struct opened_handles {
	int fh;							/*!< File handle. */

	char path[255];					/*!< Complete path of the given file handle. */
	int references;					/*!< Counter for the number of references. */
	#ifdef PVFS
	pvfs2_file_object pvfs_file;	/*!< PVFS file object to access the file. */
	#endif

	UT_hash_handle hh;				/*!< To handle the file. */
	UT_hash_handle hh_pvfs;			/*!< To handle the file in PVFS. */
};

// Structure to store statistics of requests in each forwarding
struct forwarding_statistics {
	unsigned long int open;			/*!< Number of open operations. */
	unsigned long int read;			/*!< Number of read operations. */
	unsigned long int write;		/*!< Number of write operations. */
	unsigned long int close;		/*!< Number of close operations. */

	unsigned long int read_size;	/*!< Total size of read requests. */
	unsigned long int write_size;	/*!< Total size of write requests. */
};
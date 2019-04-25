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
#include "uthash.h"

#include "main.h"

const int MAX_STRING = 100;
const int EMPTY = 0;

int simulation_forwarders = 0;

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

// Struture to keep track of open file handles
struct opened_handles {
	int fh;

	char path[255];
	int references;

	UT_hash_handle hh;
};

// Declares the hash to hold the requests and initialize it to NULL (mandatory to initialize to NULL)
struct forwarding_request *requests = NULL;
struct opened_handles *opened_files = NULL;

int world_size, world_rank;

// AGIOS client structure with pointers to callbacks
struct client agios_client;

int global_id = 1000000;
pthread_mutex_t global_id_lock;

// TODO: we may need a lock here or replace this identifier by the ID of the request
unsigned long generate_identifier() {
	// Calculates the hash of this request
	/*struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	unsigned long id = ts.tv_sec * 1000000L + ts.tv_nsec / 1000;

	return id;*/

	pthread_mutex_lock(&global_id_lock);
	global_id++;

	if (global_id > INT_MAX - 10) {
		global_id = 1000000;
	}
	pthread_mutex_unlock(&global_id_lock);

	return global_id;
}

// Controle the shutdown signal
int shutdown = 0;
pthread_mutex_t shutdown_lock;

pthread_mutex_t requests_lock;
pthread_mutex_t handles_lock;

void callback(unsigned long long int id) {
	int ack = 1;
	struct forwarding_request *r;

	#ifdef DEBUG
	// Discover the rank that sent us the message
	pthread_mutex_lock(&requests_lock);
	printf("Pending requests: %u\n", HASH_COUNT(requests));
	pthread_mutex_unlock(&requests_lock);
	
	printf("Request ID: %llu\n", id);
	#endif

	// Get the request from the hash
	pthread_mutex_lock(&requests_lock);
	HASH_FIND_INT(requests, &id, r);
	pthread_mutex_unlock(&requests_lock);

	if (r == NULL) {
		MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_REQUEST_ID);

		return;
	}

	#ifdef DEBUG
	printf("[XX][%d] %d %d %ld %ld\n", 0, r->operation, r->file_handle, r->offset, r->size);
	#endif

	// Issue the request to the filesystem
	if (r->operation == WRITE) {
		// Seek the offset
		if (lseek(r->file_handle, r->offset, SEEK_SET) == -1) {
			MPI_Abort(MPI_COMM_WORLD, ERROR_SEEK_FAILED);
		}

		// Write the file
		int rc = write(r->file_handle, r->buffer, r->size);
		
		if (rc == -1) {
			MPI_Abort(MPI_COMM_WORLD, ERROR_WRITE_FAILED);
		}
		
		// Release the AGIOS request
		//agios_release_request(r->file_handle, r->operation, r->size, r->offset, 0, r->size); // 0 is a sub-request

		// Send ACK to the client to indicate the operation was completed
		MPI_Send(&ack, 1, MPI_INT, r->rank, TAG_ACK, MPI_COMM_WORLD); 
	} else if (r->operation == READ) {
		// Seek the offset
		if (lseek(r->file_handle, r->offset, SEEK_SET) == -1) {
			MPI_Abort(MPI_COMM_WORLD, ERROR_SEEK_FAILED);
		}

		// Read the file
		int rc = read(r->file_handle, r->buffer, r->size);
		
		if (rc == -1) {
			MPI_Abort(MPI_COMM_WORLD, ERROR_READ_FAILED);
		}

		MPI_Send(r->buffer, r->size, MPI_CHAR, r->rank, TAG_BUFFER, MPI_COMM_WORLD); 
	}

	// Remove the request from the hash
	pthread_mutex_lock(&requests_lock);
	HASH_DEL(requests, r);
	pthread_mutex_unlock(&requests_lock);

	free(r);
}

void stop_AGIOS() {
	#ifdef DEBUG
	printf("stopping AGIOS scheduling library\n");
	#endif

	agios_exit();
}

void start_AGIOS() {
	agios_client.process_request = (void *) callback;
	// agios_client.process_requests = callback_aggregated;

	// Check if AGIOS was successfully inicialized
	if (agios_init(&agios_client, AGIOS_CONFIGURATION, simulation_forwarders) != 0) {
		printf("Unable to initialize AGIOS scheduling library\n");

		stop_AGIOS();
	}
}

int get_forwarding_server() {
	// We need to split the clients between the forwarding servers
	return (world_rank - simulation_forwarders) / ((world_size - simulation_forwarders) / simulation_forwarders);
}

unsigned long int total_requests = 0;
pthread_mutex_t total_requests_lock;

void *server_listen(void *p) {
	int i = 0, flag = 0;
	int ack = 1;

	MPI_Datatype request_datatype;
	int block_lengths[5] = {255, 1, 1, 1, 1};
	MPI_Datatype type[5] = {MPI_CHAR, MPI_INT, MPI_INT, MPI_UNSIGNED_LONG, MPI_UNSIGNED_LONG};
	MPI_Aint displacement[5];

	MPI_Request request;

	struct request req;

	// Compute displacements of structure components
	MPI_Get_address(&req, displacement);
	MPI_Get_address(&req.file_handle, displacement + 1);
	MPI_Get_address(&req.operation, displacement + 2);
	MPI_Get_address(&req.offset, displacement + 3);
	MPI_Get_address(&req.size, displacement + 4);
	
	MPI_Aint base = displacement[0];

	for (i = 0; i < 5; i++) {
		displacement[i] = MPI_Aint_diff(displacement[i], base); 
	}

	MPI_Type_create_struct(5, block_lengths, displacement, type, &request_datatype); 
	MPI_Type_commit(&request_datatype);

	MPI_Status status;

	#ifdef DEBUG
	printf("LISTENING...\n");
	#endif

	char fh_str[255];

	// Listen for incoming requests
	while (1) {
		// Receive the message
		// MPI_Recv(&req, 1, request_datatype, MPI_ANY_SOURCE, TAG_REQUEST, MPI_COMM_WORLD, &status);

		MPI_Irecv(&req, 1, request_datatype, MPI_ANY_SOURCE, TAG_REQUEST, MPI_COMM_WORLD, &request);

		MPI_Test(&request, &flag, &status);

		while (!flag) {
			// While we do not receive a message we need to continously check for shutdown status

			// If all the nodes requested a shutdown, we can proceed
			if (shutdown == (world_size - simulation_forwarders) / simulation_forwarders) {
				#ifdef DEBUG
				printf("SHUTDOWN\n");
				#endif

				// We need to cancel the MPI_Irecv
				MPI_Cancel(&request);

				return NULL;
			}

			MPI_Test(&request, &flag, &status);
		}
		
		// We hace received a message as we have passed the loop

		// Keep track of the number of requests
		pthread_mutex_lock(&total_requests_lock);
		total_requests++;
		pthread_mutex_unlock(&total_requests_lock);

		MPI_Get_count(&status, request_datatype, &i);

		#ifdef DEBUG
		// Discover the rank that sent us the message
		printf("Received message from %d with length %d\n", status.MPI_SOURCE, i);
		#endif

		// Empty message means we are requests to shutdown
		if (i == 0) {
			#ifdef DEBUG
			printf("Process %d has finished\n", status.MPI_SOURCE);
			#endif

			pthread_mutex_lock(&shutdown_lock);
			shutdown++;
			pthread_mutex_unlock(&shutdown_lock);

			continue;
		}

		#ifdef DEBUG
		printf("[  ][%d] %d %d %ld %ld\n", status.MPI_SOURCE, req.operation, req.file_handle, req.offset, req.size);
		#endif

		// Create the request
		struct forwarding_request *r = (struct forwarding_request *) malloc(sizeof(struct forwarding_request));

		if (r == NULL) {
			MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
		}

		r->id = generate_identifier();
		r->operation = req.operation;
		r->rank = status.MPI_SOURCE;
		strcpy(r->file_name, req.file_name);
		r->file_handle = req.file_handle;
		r->offset = req.offset;
		r->size = req.size;

		#ifdef DEBUG
		printf("OPERATION: %d\n", r->operation);
		#endif

		// We do not schedule OPEN and CLOSE requests so we can process them now
		if (r->operation == OPEN) {
			struct opened_handles *h;

			// Check if the file is already opened
			pthread_mutex_lock(&handles_lock);
			HASH_FIND_STR(opened_files, r->file_name, h);

			if (h == NULL) {
				#ifdef DEBUG
				printf("OPEN FILE: %s\n", r->file_name);
				#endif

				// Open the file
				int fh = open(r->file_name, O_CREAT | O_RDWR, 0666);
				
				h = (struct opened_handles *) malloc(sizeof(struct opened_handles));

				if (h == NULL) {
					MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
				}

				// Saves the file handle in the hash
				h->fh = fh;
				strcpy(h->path, r->file_name);
				h->references = 1;

				HASH_ADD_STR(opened_files, path, h);
			} else {
				struct opened_handles *tmp = (struct opened_handles *) malloc(sizeof(struct opened_handles));

				if (tmp == NULL) {
					MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
				}
				
				// We need to increment the number of users of this handle
				h->references = h->references + 1;
				
				HASH_REPLACE_STR(opened_files, path, h, tmp);
				
				#ifdef DEBUG
				printf("FILE: %s\n", r->file_name);
				#endif
			}

			// Release the lock that guaranteed an atomic update
			pthread_mutex_unlock(&handles_lock);

			if (h == NULL) {
				MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
			}

			#ifdef DEBUG
			printf("FILE HANDLE: %d (references = %d)\n", h->fh, h->references);
			#endif

			// Return the handle to be used in future operations
			MPI_Send(&h->fh, 1, MPI_INT, r->rank, TAG_HANDLE, MPI_COMM_WORLD);

			continue;
		}

		// Process the READ request
		if (r->operation == READ) {
			// Allocate the buffer
			r->buffer = malloc(r->size * sizeof(char));

			// Include the request into the hash list
			pthread_mutex_lock(&requests_lock);
			HASH_ADD_INT(requests, id, r);
			pthread_mutex_unlock(&requests_lock);

			#ifdef DEBUG
			printf("add (handle: %d, operation: %d, offset: %ld, size: %ld, id: %ld)\n", r->file_handle, r->operation, r->offset, r->size, r->id);
			#endif

			sprintf(fh_str, "%015d\n", r->file_handle);

			// Send the request to AGIOS
			if (agios_add_request(fh_str, r->operation, r->offset, r->size, (void *) r->id, &agios_client, 0)) {
				// Failed to sent to AGIOS, we should remove the request from the list
				printf("Failed to send the request to AGIOS\n");

				MPI_Abort(MPI_COMM_WORLD, ERROR_AGIOS_REQUEST);
			}

			continue;
		} 
		
		// Process the WRITE request
		if (r->operation == WRITE) {
			// Make sure the buffer can store the message
			r->buffer = malloc(r->size * sizeof(char));

			#ifdef DEBUG
			printf("waiting to receive the buffer [id=%ld]...\n", r->id);
			#endif

			MPI_Irecv(r->buffer, r->size, MPI_CHAR, r->rank, r->id, MPI_COMM_WORLD, &request); 

			// Send ACK to receive the buffer with the request ID
			MPI_Send(&r->id, 1, MPI_INT, r->rank, TAG_ACK, MPI_COMM_WORLD); 

			// Wait until we receive the buffer
			MPI_Wait(&request, &status);

			int size = 0;

			// Get the size of the received message
			MPI_Get_count(&status, MPI_CHAR, &size);

			// Make sure we received all the buffer
			assert(r->size == size);

			// Include the request into the hash list
			pthread_mutex_lock(&requests_lock);
			HASH_ADD_INT(requests, id, r);
			pthread_mutex_unlock(&requests_lock);

			#ifdef DEBUG
			printf("add (handle: %d, operation: %d, offset: %ld, size: %ld, id: %ld)\n", r->file_handle, r->operation, r->offset, r->size, r->id);
			#endif

			sprintf(fh_str, "%015d\n", r->file_handle);

			// Send the request to AGIOS
			if (agios_add_request(fh_str, r->operation, r->offset, r->size, (void *) r->id, &agios_client, 0)) {
				// Failed to sent to AGIOS, we should remove the request from the list
				printf("Failed to send the request to AGIOS\n");

				MPI_Abort(MPI_COMM_WORLD, ERROR_AGIOS_REQUEST);
			}

			continue;
		}

		// We do not schedule OPEN and CLOSE requests so we can process them now
		if (r->operation == CLOSE) {
			struct opened_handles *h;

			// Check if the file is already opened
			pthread_mutex_lock(&handles_lock);
			HASH_FIND_STR(opened_files, r->file_name, h);

			if (h == NULL) {
				MPI_Abort(MPI_COMM_WORLD, ERROR_FAILED_TO_CLOSE);
			} else {
				// Update the number of users
				h->references = h->references - 1;

				// Check if we can actually close the file
				if (h->references == 0) {
					#ifdef DEBUG
					printf("CLOSED: %s (%d)\n", h->path, h->fh);
					#endif

					// Close the file
					close(h->fh);

					// Remove the request from the hash
					HASH_DEL(opened_files, h);

					free(h);
				} else {
					struct opened_handles *tmp = (struct opened_handles *) malloc(sizeof(struct opened_handles));

					if (tmp == NULL) {
						MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
					}

					HASH_REPLACE_STR(opened_files, path, h, tmp);

					#ifdef DEBUG
					printf("FILE HANDLE: %d (references = %d)\n", h->fh, h->references);
					#endif
				}				
			}

			// Release the lock that guaranteed an atomic update
			pthread_mutex_unlock(&handles_lock);

			// Return the handle to be used in future operations
			MPI_Send(&ack, 1, MPI_INT, r->rank, TAG_ACK, MPI_COMM_WORLD);

			continue;
		}
		
		// Handle unknown request type
		MPI_Abort(MPI_COMM_WORLD, ERROR_UNKNOWN_REQUEST_TYPE);
	}

	//#ifdef DEBUG
	pthread_mutex_lock(&total_requests_lock);
	printf("TOTAL REQUESTS: %ld\n", total_requests);
	pthread_mutex_unlock(&total_requests_lock);
	//#endif

	return NULL;
}

static int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
	if (tok->type == JSMN_STRING && (int) strlen(s) == tok->end - tok->start && strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
		return 0;
	}

	return -1;
}

int main(int argc, char *argv[]) {
	int i, n, my_forwarding_server, ack, request_id;
	int provided;

	struct timespec start_time, end_time;
	
	double elapsed;
	double *rank_elapsed;

	double elapsed_min, elapsed_q1, elapsed_mean, elapsed_q2, elapsed_q3, elapsed_max;

	MPI_Status status;

	char *buffer = malloc(MAXIMUM_REQUEST_SIZE * sizeof(char));

	if (buffer == NULL) {
		printf("ERROR: Unable to allocate the maximum memory size of %d bytes for requests \n", MAXIMUM_REQUEST_SIZE);

		MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
	}

	// Start up MPI
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE , &provided);
	
		// Make sure MPI has thread support
	if (provided != MPI_THREAD_MULTIPLE) {
		printf("ERROR: the MPI library doesn't provide the required thread level\n");

		MPI_Abort(MPI_COMM_WORLD, ERROR_UNSUPPORTED);
	}

	// Get the number of processes
	MPI_Comm_size(MPI_COMM_WORLD, &world_size); 

	// Get my rank among all the processes
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank); 

	// Get the name of the processor
	char processor_name[MPI_MAX_PROCESSOR_NAME];
	int name_len;
	MPI_Get_processor_name(processor_name, &name_len);

	#ifdef DEBUG
	// Print off a hello world message
	printf("Hello world from processor %s, rank %d out of %d processors\n", processor_name, world_rank, world_size);
	#endif

	FILE *json_file;
	char *configuration;
	long bytes;
	 
	// Open the JSON configuration file for reading
	json_file = fopen("simulation.json", "r");
	 
	// Quit if the file does not exist
	if (json_file == NULL) {
		return 1;
	}
	 
	// Get the number of bytes in the file
	fseek(json_file, 0L, SEEK_END);
	bytes = ftell(json_file);
	 
	// Reset the file position indicator to the beginning of the file
	fseek(json_file, 0L, SEEK_SET);	
	 
	// Allocate sufficient memory for the buffer to hold the text
	configuration = (char*) calloc(bytes, sizeof(char));
	 
	// Check for memory allocation error
	if (configuration == NULL) {
		return 1;
	}
	 
	// Copy all the text into the configuration
	fread(configuration, sizeof(char), bytes, json_file);

	// Closes the JSON configuration file
	fclose(json_file);

	jsmn_parser parser;
	jsmntok_t tokens[512];

	jsmn_init(&parser);

	int ret = jsmn_parse(&parser, configuration, strlen(configuration), tokens, sizeof(tokens) / sizeof(tokens[0]));
	if (ret < 0) {
		printf("Failed to parse JSON: %d\n", ret);
		
		MPI_Abort(MPI_COMM_WORLD, ERROR_FAILED_TO_PARSE_JSON);
	}

	// Assume the top-level element is an object
	if (ret < 1 || tokens[0].type != JSMN_OBJECT) {
		printf("Object expected\n");
		
		MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_JSON);
	}

	// Simulation configuration
	char *simulation_file;
	char *simulation_spatiality_name;

	int simulation_spatiality;
	unsigned long simulation_request_size;
	unsigned long simulation_total_size;
	unsigned long simulation_rank_size;

	unsigned long b;

	// Loop over all keys of the root object
	for (i = 1; i < ret; i++) {
		if (jsoneq(configuration, &tokens[i], "forwarders") == 0) {
			simulation_forwarders = atoi(strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start));

			i++;
		} else if (jsoneq(configuration, &tokens[i], "file") == 0) {
			simulation_file = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);

			i++;
		} else if (jsoneq(configuration, &tokens[i], "spatiality") == 0) {
			simulation_spatiality_name = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);

			i++;

			// Check for supported spatialities
			if (strcmp("contiguous", simulation_spatiality_name) != 0) {
				simulation_spatiality = CONTIGUOUS;
			} else if (strcmp("strided", simulation_spatiality_name) != 0) {
				simulation_spatiality = STRIDED;
			} else {
				// Handle unkown patterns
				MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_PATTERN);
			}			
		} else if (jsoneq(configuration, &tokens[i], "total_size") == 0) {
			simulation_total_size = strtoul(strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start), NULL, 10);

			i++;
		} else if (jsoneq(configuration, &tokens[i], "request_size") == 0) {
			simulation_request_size = strtoul(strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start), NULL, 10);

			i++;
		} else {
			printf("Unknown key: %.*s (ignored)\n", tokens[i].end - tokens[i].start, configuration + tokens[i].start);
		}
	}

	// Before proceeding we need to make sure we have the minimum number of processes to simulate
	if (world_size < simulation_forwarders * 2) {
		// In case we do not have at least one client per forwarder we must stop
		MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_SETUP);
	}

	// Make sure we have a balanced number of clients for each forwading server
	if (world_size % simulation_forwarders) {
		MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_SETUP);
	}

	int is_forwarding = 0;

	// We need to split the processes into forwarding servers and the simulated clients
	if (world_rank < simulation_forwarders) {
		is_forwarding = 1;
	}

	// Communicator for the processes that are a part of the forwarding server
	MPI_Comm forwarding_comm;
	MPI_Comm_split(MPI_COMM_WORLD, is_forwarding, world_rank, &forwarding_comm);

	int forwarding_rank, forwarding_size;
	MPI_Comm_rank(forwarding_comm, &forwarding_rank);
	MPI_Comm_size(forwarding_comm, &forwarding_size);

	// Communicator for the processes that are forwarding clients
	MPI_Comm clients_comm;
	MPI_Comm_split(MPI_COMM_WORLD, !is_forwarding, world_rank, &clients_comm);

	int client_rank, client_size;
	MPI_Comm_rank(clients_comm, &client_rank);
	MPI_Comm_size(clients_comm, &client_size);

	MPI_File fh;
	MPI_Status s;

	// Snapshot of the simulation configuration
	MPI_File_open(MPI_COMM_WORLD, "simulation.map", MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

	char map[1024];
	sprintf(map, "rank %d: %s\n", world_rank, (is_forwarding ? "server" : "client"));

	// Write the subarray
	MPI_File_write_ordered(fh, &map, strlen(map), MPI_CHAR, &s);

	// Close the file
	MPI_File_close(&fh);

	MPI_Barrier(MPI_COMM_WORLD);

	MPI_Datatype request_datatype;
	int block_lengths[5] = {255, 1, 1, 1, 1};
	MPI_Datatype type[5] = {MPI_CHAR, MPI_INT, MPI_INT, MPI_UNSIGNED_LONG, MPI_UNSIGNED_LONG};
	MPI_Aint displacement[5];

	struct request req;

	// Compute displacements of structure components
	MPI_Get_address(&req, displacement);
	MPI_Get_address(&req.file_handle, displacement + 1);
	MPI_Get_address(&req.operation, displacement + 2);
	MPI_Get_address(&req.offset, displacement + 3);
	MPI_Get_address(&req.size, displacement + 4);
	
	MPI_Aint base = displacement[0];

	for (i = 0; i < 5; i++) {
		displacement[i] = MPI_Aint_diff(displacement[i], base); 
	}

	MPI_Type_create_struct(5, block_lengths, displacement, type, &request_datatype); 
	MPI_Type_commit(&request_datatype);

	if (is_forwarding) {
		start_AGIOS();

		// Create threads to list for requests
		pthread_t listen[FWD_LISTEN_THREADS];

		for (i = 0; i < FWD_LISTEN_THREADS; i++) {
			pthread_create(&listen[i], NULL, server_listen, NULL);
		}

		for (i = 0; i < FWD_LISTEN_THREADS; i++) {
			pthread_join(listen[i], NULL);
		}

		stop_AGIOS();
	} else {
		int forwarding_fh;

		struct request *r;

		// Define the forwarding server we should interact with
		my_forwarding_server = get_forwarding_server();

		#ifdef DEBUG
		printf("RANK %d\tFORWARDER SERVER: %d\n", world_rank, my_forwarding_server);

		printf("sending message from process %d of %d!\n", world_rank, world_size);
		#endif

		// Define the size per process
		simulation_rank_size = ceil(simulation_total_size / client_size);

		n = simulation_rank_size / simulation_request_size;

		// Fill the buffer with fake data to be written
		for (b = 0; b < simulation_request_size; b++) {
			buffer[b] = '0' + world_rank;
		}

		if (client_rank == 0) {
			MPI_File_open(MPI_COMM_SELF, "statistics.time", MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

			sprintf(map, "---------------------------\n I/O Forwarding Simulation\n---------------------------\n");
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);

			sprintf(map, " forwarders: %13d\n clients:    %13d\n spatiality: %13d\n request:    %13ld\n total:      %13ld\n---------------------------\n\n", forwarding_size, world_size, simulation_spatiality, simulation_request_size, simulation_total_size);
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);
		}

		/*
		 * OPEN ------------------------------------------------------------------------------------
		 * Issue OPEN request to the forwarding layer
		 * -----------------------------------------------------------------------------------------
		 */

		// Create the request
		r = (struct request *) malloc(sizeof(struct request));

		r->operation = OPEN;
		strcpy(r->file_name, simulation_file);
		r->file_handle = -1;
		r->offset = 0;
		r->size = 1;

		// Issue the OPEN operation, that should wait for it to complete
		MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

		#ifdef DEBUG
		printf("waiting for the file handle...\n");
		#endif

		// We need to wait for the file handle to continue
		MPI_Recv(&forwarding_fh, 1, MPI_INT, my_forwarding_server, TAG_HANDLE, MPI_COMM_WORLD, &status);

		#ifdef DEBUG
		printf("HANDLE received [id=%d] %d\n", request_id, forwarding_fh);
		#endif

		/*
		 * WRITE ------------------------------------------------------------------------------------
		 * Issue WRITE requests to the forwarding layer
		 * -----------------------------------------------------------------------------------------
		 */

		sprintf(map, " WRITE\n---------------------------\n");
		MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);

		clock_gettime(CLOCK_MONOTONIC, &start_time);

		for (i = 0; i < n; i++) {
			// Create the request
			r = (struct request *) malloc(sizeof(struct request));

			r->operation = WRITE;
			r->file_handle = forwarding_fh;

			if (simulation_spatiality == CONTIGUOUS) {
				// Offset computation is based on MPI-IO Test implementation
				r->offset = (floor(i / n) * ((simulation_request_size * n) * client_size)) + ((simulation_request_size * n) * client_rank) + (i * simulation_request_size);

			} else {
				// Offset computation is based on MPI-IO Test implementation
				r->offset = i * (client_size * simulation_request_size) + (client_rank * simulation_request_size);
			}
			
			r->size = simulation_request_size;

			#ifdef DEBUG
			printf("[OP][%d] %d %d %ld %ld\n", world_rank, r->operation, r->file_handle, r->offset, r->size);
			#endif

			// Issue the fake WRITE operation, that should wait for it to complete
			MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

			#ifdef DEBUG
			printf("waiting for the answer to come...\n");
			#endif

			// We need to wait for the ACK so that the server is ready to receive our buffer and the request ID
			MPI_Recv(&request_id, 1, MPI_INT, my_forwarding_server, TAG_ACK, MPI_COMM_WORLD, &status);

			#ifdef DEBUG
			printf("ACK received [id=%d], sending buffer...\n", request_id);
			#endif

			int send_status;

			// We need to wait for the WRITE request to return before issuing another request
			send_status = MPI_Send(buffer, r->size, MPI_CHAR, my_forwarding_server, request_id, MPI_COMM_WORLD);

			assert(send_status == MPI_SUCCESS);

			#ifdef DEBUG
			printf("sent WRITE data to %d with length %ld\n", my_forwarding_server, r->size);
			#endif

			// We need to wait for the ACK so that the server has finished to process our request
			MPI_Recv(&ack, 1, MPI_INT, my_forwarding_server, TAG_ACK, MPI_COMM_WORLD, &status);
		}

		clock_gettime(CLOCK_MONOTONIC, &end_time);

		// Execution time in nano seconds and convert it to seconds
		elapsed = ((end_time.tv_nsec - start_time.tv_nsec) + ((end_time.tv_sec - start_time.tv_sec)*1000000000L)) / 1000000000.0;
		
		MPI_Barrier(clients_comm);

		/*
		 * STATISTICS ------------------------------------------------------------------------------
		 * Collect statistics form all the client nodes
		 * -----------------------------------------------------------------------------------------
		 */

		if (client_rank == 0) {
			rank_elapsed = malloc(sizeof(double) * client_size);
		}

		MPI_Gather(&elapsed, 1, MPI_DOUBLE , rank_elapsed, 1, MPI_DOUBLE, 0, clients_comm);

		if (client_rank == 0) {
			// Snapshot of the simulation configuration
			for (i = 0; i < client_size; i++) {
				sprintf(map, " rank %03d: %15.7lf\n", i, rank_elapsed[i]);

				// Write the subarray
				MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);
			}

			sprintf(map, "---------------------------\n");
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);

			elapsed_min = gsl_stats_min(rank_elapsed, 1, client_size);
			elapsed_mean = gsl_stats_mean(rank_elapsed, 1, client_size);
			elapsed_max = gsl_stats_max(rank_elapsed, 1, client_size);

			gsl_sort(rank_elapsed, 1, client_size);

			elapsed_q1 = gsl_stats_quantile_from_sorted_data(rank_elapsed, 1, client_size, 0.25);
			elapsed_q2 = gsl_stats_median_from_sorted_data(rank_elapsed, 1, client_size);
			elapsed_q3 = gsl_stats_quantile_from_sorted_data(rank_elapsed, 1, client_size, 0.75);

			sprintf(map, "      min: %15.7lf\n       Q1: %15.7lf\n       Q2: %15.7lf\n       Q3: %15.7lf\n      max: %15.7lf\n     mean: %15.7lf\n", elapsed_min, elapsed_q1, elapsed_q2, elapsed_q3, elapsed_max, elapsed_mean);
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);

			sprintf(map, "---------------------------\n");
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);
		}
		
		MPI_Barrier(clients_comm);

		/*
		 * CLOSE -----------------------------------------------------------------------------------
		 * Issue CLOSE request to the forwarding layer
		 * -----------------------------------------------------------------------------------------
		 */

		// Create the request
		r = (struct request *) malloc(sizeof(struct request));

		r->operation = CLOSE;
		strcpy(r->file_name, simulation_file);
		r->file_handle = -1;
		r->offset = 0;
		r->size = 1;

		// Issue the CLOSE operation, that should wait for it to complete
		MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

		#ifdef DEBUG
		printf("waiting for the close ACK...\n");
		#endif

		// We need to wait for the file handle to continue
		MPI_Recv(&forwarding_fh, 1, MPI_INT, my_forwarding_server, TAG_ACK, MPI_COMM_WORLD, &status);

		MPI_Barrier(clients_comm);

		/*
		 * OPEN ------------------------------------------------------------------------------------
		 * Issue OPEN request to the forwarding layer
		 * -----------------------------------------------------------------------------------------
		 */

		// Create the request
		r = (struct request *) malloc(sizeof(struct request));

		r->operation = OPEN;
		strcpy(r->file_name, simulation_file);
		r->file_handle = -1;
		r->offset = 0;
		r->size = 1;

		// Issue the OPEN operation, that should wait for it to complete
		MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

		#ifdef DEBUG
		printf("waiting for the file handle...\n");
		#endif

		// We need to wait for the file handle to continue
		MPI_Recv(&forwarding_fh, 1, MPI_INT, my_forwarding_server, TAG_HANDLE, MPI_COMM_WORLD, &status);

		#ifdef DEBUG
		printf("HANDLE received [id=%d] %d\n", request_id, forwarding_fh);
		#endif
		
		/*
		 * READ ------------------------------------------------------------------------------------
		 * Issue READ requests to the forwarding layer
		 * -----------------------------------------------------------------------------------------
		 */

		sprintf(map, "\n READ\n---------------------------\n");
		MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);

		clock_gettime(CLOCK_MONOTONIC, &start_time);

		for (i = 0; i < n; i++) {
			// Create the request
			r = (struct request *) malloc(sizeof(struct request));

			r->operation = READ;
			r->file_handle = forwarding_fh;
			
			if (simulation_spatiality == CONTIGUOUS) {
				// Offset computation is based on MPI-IO Test implementation
				r->offset = (floor(i / n) * ((simulation_request_size * n) * client_size)) + ((simulation_request_size * n) * client_rank) + (i * simulation_request_size);
			} else {
				// Offset computation is based on MPI-IO Test implementation
				r->offset = i * (client_size * simulation_request_size) + (client_rank * simulation_request_size);
			}

			r->size = simulation_request_size;

			#ifdef DEBUG
			printf("[OP][%d] %d %d %ld %ld\n", world_rank, r->operation, r->file_handle, r->offset, r->size);
			#endif

			// Issue the fake READ operation, that should wait for it to complete
			MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

			#ifdef DEBUG
			printf("waiting for the answer to come...\n");
			#endif

			// We need to wait for the READ request to return before issuing another request
			MPI_Recv(buffer, r->size, MPI_CHAR, my_forwarding_server, TAG_BUFFER, MPI_COMM_WORLD, &status);

			int size = 0;

			// Get the size of the received message
			MPI_Get_count(&status, MPI_CHAR, &size);

			// Make sure we received all the buffer
			assert(r->size == size);

			#ifdef DEBUG
			// Discover the rank that sent us the message
			printf("received READ data from %d with length %d\n", status.MPI_SOURCE, size);
			#endif
		}

		clock_gettime(CLOCK_MONOTONIC, &end_time);

		// Execution time in nano seconds and convert it to seconds
		elapsed = ((end_time.tv_nsec - start_time.tv_nsec) + ((end_time.tv_sec - start_time.tv_sec)*1000000000L)) / 1000000000.0;
		
		MPI_Barrier(clients_comm);

		/*
		 * CLOSE -----------------------------------------------------------------------------------
		 * Issue CLOSE request to the forwarding layer
		 * -----------------------------------------------------------------------------------------
		 */

		// Create the request
		r = (struct request *) malloc(sizeof(struct request));

		r->operation = CLOSE;
		strcpy(r->file_name, simulation_file);
		r->file_handle = -1;
		r->offset = 0;
		r->size = 1;

		// Issue the CLOSE operation, that should wait for it to complete
		MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

		#ifdef DEBUG
		printf("waiting for the close ACK...\n");
		#endif

		// We need to wait for the file handle to continue
		MPI_Recv(&forwarding_fh, 1, MPI_INT, my_forwarding_server, TAG_ACK, MPI_COMM_WORLD, &status);

		MPI_Barrier(clients_comm);

		/*
		 * STATISTICS ------------------------------------------------------------------------------
		 * Collect statistics form all the client nodes
		 * -----------------------------------------------------------------------------------------
		 */

		if (client_rank == 0) {
			rank_elapsed = malloc(sizeof(double) * client_size);
		}

		MPI_Gather(&elapsed, 1, MPI_DOUBLE , rank_elapsed, 1, MPI_DOUBLE, 0, clients_comm);

		if (client_rank == 0) {
			// Snapshot of the simulation configuration
			for (i = 0; i < client_size; i++) {
				sprintf(map, " rank %03d: %15.7lf\n", i, rank_elapsed[i]);

				// Write the subarray
				MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);
			}

			sprintf(map, "---------------------------\n");
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);

			elapsed_min = gsl_stats_min(rank_elapsed, 1, client_size);
			elapsed_mean = gsl_stats_mean(rank_elapsed, 1, client_size);
			elapsed_max = gsl_stats_max(rank_elapsed, 1, client_size);

			gsl_sort(rank_elapsed, 1, client_size);

			elapsed_q1 = gsl_stats_quantile_from_sorted_data(rank_elapsed, 1, client_size, 0.25);
			elapsed_q2 = gsl_stats_median_from_sorted_data(rank_elapsed, 1, client_size);
			elapsed_q3 = gsl_stats_quantile_from_sorted_data(rank_elapsed, 1, client_size, 0.75);

			sprintf(map, "      min: %15.7lf\n       Q1: %15.7lf\n       Q2: %15.7lf\n       Q3: %15.7lf\n      max: %15.7lf\n     mean: %15.7lf\n", elapsed_min, elapsed_q1, elapsed_q2, elapsed_q3, elapsed_max, elapsed_mean);
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);

			sprintf(map, "---------------------------\n");
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);
		}

		// Free the request
		free(r);

		// Free the statistics related to each rank
		free(rank_elapsed);

		/*
		 * SHUTDOWN---------------------------------------------------------------------------------
		 * Issue SHUTDOWN requests to the forwarding layer to finish the experiment
		 * -----------------------------------------------------------------------------------------
		 */

		if (client_rank == 0) {
			// Close the statistics file
			MPI_File_close(&fh);
		}


		MPI_Barrier(clients_comm);

		#ifdef DEBUG
		printf("rank %d is sending SHUTDOWN signal\n", world_rank);
		#endif

		// Send shutdown message
		MPI_Send(buffer, EMPTY, MPI_CHAR, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD);
	}

	MPI_Barrier(MPI_COMM_WORLD);

	// Free the communicator used in the forwarding server
	MPI_Comm_free(&forwarding_comm);

	// Free the communicator used by the clients
	MPI_Comm_free(&clients_comm);

	// Free the buffer
	free(buffer);

	/* Shut down MPI */
	MPI_Finalize(); 

	return 0;
}

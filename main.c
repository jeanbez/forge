#include "main.h"
#include "log.h"

const int MAX_STRING = 100;
const int EMPTY = 0;

int simulation_forwarders = 0;

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

FWD_LIST_HEAD(ready_queue);

pthread_mutex_t ready_queue_mutex;
pthread_cond_t ready_queue_signal;

#ifdef DEBUG
struct ready_request *tmp;
#endif

void callback(unsigned long long int id) {
	// Create the ready request
	struct ready_request *ready_r = (struct ready_request *) malloc(sizeof(struct ready_request));

	ready_r->id = id;

	pthread_mutex_lock(&ready_queue_mutex);

	// Place the request in the ready queue to be issued by the processing threads
	fwd_list_add_tail(&(ready_r->list), &ready_queue);

	// Print the items on the list to make sure we have all the requests
	log_trace("CALLBACK READY QUEUE");

	#ifdef DEBUG
	fwd_list_for_each_entry(tmp, &ready_queue, list) {
		log_trace("\tcallback request: %d", tmp->id);
	}
	#endif

	// Signal the condition variable that new data is available in the queue
	pthread_cond_signal(&ready_queue_signal);

	pthread_mutex_unlock(&ready_queue_mutex);
}

void callback_aggregated(unsigned long long int *ids, int total) {
	int i;

	pthread_mutex_lock(&ready_queue_mutex);

	for (i = 0; i < total; i++) {
		// Create the ready request
		struct ready_request *ready_r = (struct ready_request *) malloc(sizeof(struct ready_request));

		ready_r->id = ids[i];

		// Place the request in the ready queue to be issued by the processing threads
		fwd_list_add_tail(&(ready_r->list), &ready_queue);

		// Print the items on the list to make sure we have all the requests
		log_trace("AGG READY QUEUE");

		#ifdef DEBUG
		fwd_list_for_each_entry(tmp, &ready_queue, list) {
			log_trace("\tAGG request: %d", tmp->id);
		}
		#endif
	}

	// Signal the condition variable that new data is available in the queue
	pthread_cond_signal(&ready_queue_signal);

	pthread_mutex_unlock(&ready_queue_mutex);
}

void stop_AGIOS() {
	log_debug("stopping AGIOS scheduling library");

	agios_exit();
}

void start_AGIOS() {
	agios_client.process_request = (void *) callback;
	//agios_client.process_requests = (void *) callback_aggregated;

	// Check if AGIOS was successfully inicialized
	if (agios_init(&agios_client, AGIOS_CONFIGURATION, simulation_forwarders) != 0) {
		log_debug("Unable to initialize AGIOS scheduling library");

		stop_AGIOS();
	}
}

int get_forwarding_server() {
	// We need to split the clients between the forwarding servers
	return (world_rank - simulation_forwarders) / ((world_size - simulation_forwarders) / simulation_forwarders);
}

struct forwarding_statistics *statistics;

pthread_mutex_t statistics_lock;

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

	log_debug("LISTEN THREAD %ld", pthread_self());

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
				log_debug("SHUTDOWN: listen %d thread %ld", world_rank, pthread_self());

				// We need to signal the processing thread to proceed and check for shutdown
				pthread_cond_signal(&ready_queue_signal);

				// We need to cancel the MPI_Irecv
				MPI_Cancel(&request);

				return NULL;
			}

			MPI_Test(&request, &flag, &status);
		}
		
		// We hace received a message as we have passed the loop

		MPI_Get_count(&status, request_datatype, &i);

		// Discover the rank that sent us the message
		log_debug("Received message from %d with length %d", status.MPI_SOURCE, i);

		// Empty message means we are requests to shutdown
		if (i == 0) {
			log_debug("Process %d has finished", status.MPI_SOURCE);

			pthread_mutex_lock(&shutdown_lock);
			shutdown++;
			pthread_mutex_unlock(&shutdown_lock);

			continue;
		}

		log_debug("[  ][%d] %d %d %ld %ld", status.MPI_SOURCE, req.operation, req.file_handle, req.offset, req.size);

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

		log_debug("OPERATION: %d", r->operation);

		// We do not schedule OPEN and CLOSE requests so we can process them now
		if (r->operation == OPEN) {
			struct opened_handles *h;

			// Check if the file is already opened
			pthread_mutex_lock(&handles_lock);
			HASH_FIND_STR(opened_files, r->file_name, h);

			if (h == NULL) {
				log_debug("OPEN FILE: %s", r->file_name);

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
				
				log_debug("FILE: %s", r->file_name);
			}

			// Release the lock that guaranteed an atomic update
			pthread_mutex_unlock(&handles_lock);

			if (h == NULL) {
				MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
			}

			log_debug("FILE HANDLE: %d (references = %d)", h->fh, h->references);

			// Return the handle to be used in future operations
			MPI_Send(&h->fh, 1, MPI_INT, r->rank, TAG_HANDLE, MPI_COMM_WORLD);

			// Update the statistics
			pthread_mutex_lock(&statistics_lock);
			statistics->open += 1;
			pthread_mutex_unlock(&statistics_lock);

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

			log_debug("add (handle: %d, operation: %d, offset: %ld, size: %ld, id: %ld)", r->file_handle, r->operation, r->offset, r->size, r->id);

			sprintf(fh_str, "%015d", r->file_handle);

			// Send the request to AGIOS
			if (agios_add_request(fh_str, r->operation, r->offset, r->size, (void *) r->id, &agios_client, 0)) {
				// Failed to sent to AGIOS, we should remove the request from the list
				log_debug("Failed to send the request to AGIOS");

				MPI_Abort(MPI_COMM_WORLD, ERROR_AGIOS_REQUEST);
			}

			// Update the statistics
			pthread_mutex_lock(&statistics_lock);
			statistics->read += 1;
			statistics->read_size += r->size;
			pthread_mutex_unlock(&statistics_lock);

			continue;
		} 
		
		// Process the WRITE request
		if (r->operation == WRITE) {
			// Make sure the buffer can store the message
			r->buffer = malloc(r->size * sizeof(char));

			log_debug("waiting to receive the buffer [id=%ld]...", r->id);

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

			log_debug("add (handle: %d, operation: %d, offset: %ld, size: %ld, id: %ld)", r->file_handle, r->operation, r->offset, r->size, r->id);

			sprintf(fh_str, "%015d", r->file_handle);

			// Send the request to AGIOS
			if (agios_add_request(fh_str, r->operation, r->offset, r->size, (void *) r->id, &agios_client, 0)) {
				// Failed to sent to AGIOS, we should remove the request from the list
				log_debug("Failed to send the request to AGIOS");

				MPI_Abort(MPI_COMM_WORLD, ERROR_AGIOS_REQUEST);
			}

			// Update the statistics
			pthread_mutex_lock(&statistics_lock);
			statistics->write += 1;
			statistics->write_size += r->size;
			pthread_mutex_unlock(&statistics_lock);

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
					log_debug("CLOSED: %s (%d)", h->path, h->fh);

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

					log_debug("FILE HANDLE: %d (references = %d)", h->fh, h->references);
				}				
			}

			// Release the lock that guaranteed an atomic update
			pthread_mutex_unlock(&handles_lock);

			// Return the handle to be used in future operations
			MPI_Send(&ack, 1, MPI_INT, r->rank, TAG_ACK, MPI_COMM_WORLD);

			// Update the statistics
			pthread_mutex_lock(&statistics_lock);
			statistics->close += 1;
			pthread_mutex_unlock(&statistics_lock);

			continue;
		}
		
		// Handle unknown request type
		MPI_Abort(MPI_COMM_WORLD, ERROR_UNKNOWN_REQUEST_TYPE);
	}

	return NULL;
}

void *server_dispatcher(void *p) {
	int request_id;

	int ack = 1;
	struct forwarding_request *r;
	struct ready_request *ready_r;

	char fh_str[255];

	log_debug("DISPATCHER THREAD %ld", pthread_self());

	while (1) {
		// Start by locking the queue mutex
		pthread_mutex_lock(&ready_queue_mutex);

		// While the queue is empty wait for the condition variable to be signalled
		while (fwd_list_empty(&ready_queue)) {
			// This call unlocks the mutex when called and relocks it before returning!
			pthread_cond_wait(&ready_queue_signal, &ready_queue_mutex);

			// Check for shutdown signal
			if (shutdown == (world_size - simulation_forwarders) / simulation_forwarders) {
				// Unlock the queue mutex to allow other threads to complete
				pthread_mutex_unlock(&ready_queue_mutex);

				// We need to signal the processing thread to proceed and check for shutdown
				pthread_cond_signal(&ready_queue_signal);

				log_info("SHUTDOWN: dispatcher %d thread %ld", world_rank, pthread_self());

				return NULL;
			}
		}

		// There must be data in the ready queue, so we can get it
		ready_r = fwd_list_entry(ready_queue.next, struct ready_request, list);

		// Fetch the request ID
		request_id = ready_r->id;

		log_debug("[P-%ld] ---> ISSUE: %d", pthread_self(), request_id);

		// Remove it from the queue
		fwd_list_del(&ready_r->list);

		free(ready_r);

		// Unlock the queue mutex
		pthread_mutex_unlock(&ready_queue_mutex);

		// Process the request
		#ifdef DEBUG
		// Discover the rank that sent us the message
		pthread_mutex_lock(&requests_lock);
		log_debug("[P-%ld] Pending requests: %u", pthread_self(), HASH_COUNT(requests));
		pthread_mutex_unlock(&requests_lock);
		
		log_debug("[P-%ld] Request ID: %u", pthread_self(), request_id);
		#endif

		// Get the request from the hash
		pthread_mutex_lock(&requests_lock);
		HASH_FIND_INT(requests, &request_id, r);
		pthread_mutex_unlock(&requests_lock);

		if (r == NULL) {
			MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_REQUEST_ID);

			return NULL;
		}			

		log_debug("[XX][%d] %d %d %ld %ld", 0, r->operation, r->file_handle, r->offset, r->size);

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

			// Send ACK to the client to indicate the operation was completed
			MPI_Send(&ack, 1, MPI_INT, r->rank, TAG_ACK, MPI_COMM_WORLD); 

			// Release the AGIOS request
			sprintf(fh_str, "%015d", r->file_handle);

			agios_release_request(fh_str, r->operation, r->size, r->offset, 0, r->size); // 0 is a sub-request
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

			// Release the AGIOS request
			sprintf(fh_str, "%015d", r->file_handle);

			agios_release_request(fh_str, r->operation, r->size, r->offset, 0, r->size); // 0 is a sub-request
		}
		
		// Remove the request from the hash
		pthread_mutex_lock(&requests_lock);
		HASH_DEL(requests, r);
		pthread_mutex_unlock(&requests_lock);

		// Free the buffer and the request
		free(r->buffer);
		free(r);
	}

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

	log_set_level(LOG_INFO);

	#ifdef DEBUG
	log_set_level(LOG_TRACE);
	#endif

	if (argc != 2) {
		printf("Usage: ./fwd-sim <simulation.json>\n");

		return 0;
	}

	char *buffer = malloc(MAXIMUM_REQUEST_SIZE * sizeof(char));

	if (buffer == NULL) {
		log_error("ERROR: Unable to allocate the maximum memory size of %d bytes for requests ", MAXIMUM_REQUEST_SIZE);

		MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
	}

	// Start up MPI
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE , &provided);
	
		// Make sure MPI has thread support
	if (provided != MPI_THREAD_MULTIPLE) {
		log_error("ERROR: the MPI library doesn't provide the required thread level");

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

	// Print off a hello world message
	log_debug("Hello world from processor %s, rank %d out of %d processors", processor_name, world_rank, world_size);

	FILE *json_file;
	char *configuration;
	long bytes;

	char *base_file = argv[1];
	 
	// Open the JSON configuration file for reading
	json_file = fopen(base_file, "r");
	 
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
	configuration = (char*) calloc(bytes, sizeof(char *));
	 
	// Check for memory allocation error
	if (configuration == NULL) {
		return 1;
	}

	// Copy all the text into the configuration
	fread(configuration, sizeof(char), bytes, json_file);

	// Closes the JSON configuration file
	fclose(json_file);

	jsmn_parser parser;
	jsmntok_t tokens[1024];

	jsmn_init(&parser);

	int ret = jsmn_parse(&parser, configuration, strlen(configuration), tokens, sizeof(tokens) / sizeof(tokens[0]));
	if (ret < 0) {
		log_error("Failed to parse JSON: %d", ret);
		
		MPI_Abort(MPI_COMM_WORLD, ERROR_FAILED_TO_PARSE_JSON);
	}

	// Assume the top-level element is an object
	if (ret < 1 || tokens[0].type != JSMN_OBJECT) {
		log_error("Object expected");
		
		MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_JSON);
	}

	// Simulation configuration
	char *simulation_path;
	char *simulation_files_name;
	char *simulation_spatiality_name;

	char simulation_map_file[255];
	char simulation_time_file[255];
	char simulation_stats_file[255];

	int simulation_listeners;
	int simulation_dispatchers;

	int simulation_files;
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
		} else if (jsoneq(configuration, &tokens[i], "listeners") == 0) {
			simulation_listeners = atoi(strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start));

			i++;
		} else if (jsoneq(configuration, &tokens[i], "dispatchers") == 0) {
			simulation_dispatchers = atoi(strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start));

			i++;
		} else if (jsoneq(configuration, &tokens[i], "path") == 0) {
			simulation_path = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);

			i++;
		} else if (jsoneq(configuration, &tokens[i], "number_of_files") == 0) {
			simulation_files_name = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);

			i++;

			// Check for supported spatialities
			if (strcmp("shared", simulation_files_name) == 0) {
				simulation_files = SHARED;
			} else if (strcmp("individual", simulation_files_name) == 0) {
				simulation_files = INDIVIDUAL;
			} else {
				// Handle unkown patterns
				MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_PATTERN);
			}
		} else if (jsoneq(configuration, &tokens[i], "spatiality") == 0) {
			simulation_spatiality_name = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);

			i++;

			// Check for supported spatialities
			if (strcmp("contiguous", simulation_spatiality_name) == 0) {
				simulation_spatiality = CONTIGUOUS;
			} else if (strcmp("strided", simulation_spatiality_name) == 0) {
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
			log_warn("Unknown key: %.*s (ignored)", tokens[i].end - tokens[i].start, configuration + tokens[i].start);
		}
	}

	free(configuration);

	// Verify for an invalid pattern: individual files with 1D strided accesses
	if (simulation_files == INDIVIDUAL && simulation_spatiality == STRIDED) {
		log_error("invalid access pattern");

		MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_PATTERN);
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

	// Make sure the number of listeners is within limits
	if (simulation_listeners > FWD_MAX_LISTEN_THREADS) {
		MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_SETUP);
	}

	// Make sure the number of dispatchers is within limits
	if (simulation_dispatchers > FWD_MAX_PROCESS_THREADS) {
		MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_SETUP);
	}

	int is_forwarding = 0;

	// We need to split the processes into forwarding servers and the simulated clients
	if (world_rank < simulation_forwarders) {
		is_forwarding = 1;
	}

	// Communicator for the processes that are a part of the forwarding server

	// Get the group of processes in MPI_COMM_WORLD
	MPI_Group world_group;
	MPI_Group forwarding_group;
	MPI_Group clients_group;

	MPI_Comm forwarding_comm;
	MPI_Comm clients_comm;

	int *forwarding_ranks;
	int *client_ranks;

	// Make a list of processes in the new communicator
	forwarding_ranks = (int*) malloc(simulation_forwarders * sizeof(int));

	for (i = 0; i < simulation_forwarders; i++) {
		forwarding_ranks[i] = i;

		#ifdef DEBUG
		if (world_rank == 0) {
			printf("fwd[%d] = %d\n", i, i);
		}
		#endif
	}

	// Make a list of processes in the new communicator
	client_ranks = (int*) malloc((world_size - simulation_forwarders) * sizeof(int));

	for (i = simulation_forwarders; i < world_size; i++) {
		client_ranks[i - simulation_forwarders] = i;

		#ifdef DEBUG
		if (world_rank == 0) {
			printf("cli[%d] = %d\n", i - simulation_forwarders, i);
		}
		#endif
	}
 
	// Get the group under MPI_COMM_WORLD
	MPI_Comm_group(MPI_COMM_WORLD, &world_group);

	// Create the new group for the forwarding servers
	MPI_Group_incl(world_group, simulation_forwarders, forwarding_ranks, &forwarding_group);
	// Create the new group for the forwarding servers
	MPI_Group_incl(world_group, world_size - simulation_forwarders, client_ranks, &clients_group);

	// Create the new communicator for the forwarding servers
	MPI_Comm_create(MPI_COMM_WORLD, forwarding_group, &forwarding_comm);
	// create the new communicator for the forwarding servers
	MPI_Comm_create(MPI_COMM_WORLD, clients_group, &clients_comm);

	free(forwarding_ranks);
	free(client_ranks);

	int forwarding_rank, forwarding_size;
	if (forwarding_comm != MPI_COMM_NULL) {
		MPI_Comm_rank(forwarding_comm, &forwarding_rank);
		MPI_Comm_size(forwarding_comm, &forwarding_size);
	}

	int client_rank, client_size;
	if (clients_comm != MPI_COMM_NULL) {
		MPI_Comm_rank(clients_comm, &client_rank);
		MPI_Comm_size(clients_comm, &client_size);
	} 

	MPI_Barrier(MPI_COMM_WORLD);

	MPI_File fh;
	MPI_Status s;

	sprintf(simulation_map_file, "%s.map", base_file);
	sprintf(simulation_time_file, "%s.time", base_file);
	sprintf(simulation_stats_file, "%s.stats", base_file);

	// Snapshot of the simulation configuration
	MPI_File_open(MPI_COMM_WORLD, simulation_map_file, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

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
		// Initialize the list with the ready requests
		init_fwd_list_head(&ready_queue);

		// Start the AGIOS scheduling library
		start_AGIOS();

		// Create threads to list for requests
		pthread_t listen[FWD_MAX_LISTEN_THREADS];
		pthread_t dispatcher[FWD_MAX_PROCESS_THREADS];

		statistics = (struct forwarding_statistics *) malloc(sizeof(struct forwarding_statistics));

		statistics->open = 0;
		statistics->read = 0;
		statistics->write = 0;
		statistics->close = 0;

		statistics->read_size = 0;
		statistics->write_size = 0;

		// Create threads to issue the requests
		for (i = 0; i < simulation_dispatchers; i++) {
			pthread_create(&dispatcher[i], NULL, server_dispatcher, NULL);
		}

		for (i = 0; i < simulation_listeners; i++) {
			pthread_create(&listen[i], NULL, server_listen, NULL);
		}

		// Wait for the intitialization of servers and clients to complete
		MPI_Barrier(MPI_COMM_WORLD);

		// Wait for listen threads to complete
		for (i = 0; i < simulation_listeners; i++) {
			pthread_join(listen[i], NULL);
		}

		// Wait for processing threads to complete
		for (i = 0; i < simulation_dispatchers; i++) {
			pthread_join(dispatcher[i], NULL);
		}

		MPI_Barrier(forwarding_comm);

		// Stops the AGIOS scheduling library
		stop_AGIOS();

		MPI_Barrier(forwarding_comm);

		// Write the forwarding statistics to file
		MPI_File_open(forwarding_comm, simulation_stats_file, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

		char stats[1048576];

		sprintf(stats, "forwarder: %d\nopen: %ld\nread: %ld\nwrite: %ld\nclose: %ld\nread_size: %ld\nwrite_size: %ld\n\n", forwarding_rank, statistics->open, statistics->read, statistics->write, statistics->close, statistics->read_size, statistics->write_size);		

		// Write the subarray
		MPI_File_write_ordered(fh, &stats, strlen(stats), MPI_CHAR, &s);

		// Close the file
		MPI_File_close(&fh);
	} else {
		int forwarding_fh;

		struct request *r = NULL;

		// Define the forwarding server we should interact with
		my_forwarding_server = get_forwarding_server();

		log_debug("RANK %d\tFORWARDER SERVER: %d", world_rank, my_forwarding_server);
		log_debug("sending message from process %d of %d!", world_rank, world_size);

		// Define the size per process
		simulation_rank_size = ceil(simulation_total_size / client_size);

		n = simulation_rank_size / simulation_request_size;

		// Fill the buffer with fake data to be written
		for (b = 0; b < simulation_request_size; b++) {
			buffer[b] = '0' + world_rank;
		}

		if (client_rank == 0) {
			MPI_File_open(MPI_COMM_SELF, simulation_time_file, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

			sprintf(map, "---------------------------\n I/O Forwarding Simulation\n---------------------------\n");
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);

			sprintf(map, " forwarders: %13d\n clients:    %13d\n spatiality: %13d\n request:    %13ld\n total:      %13ld\n---------------------------\n\n", world_size - client_size, client_size, simulation_spatiality, simulation_request_size, simulation_total_size);
			MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);
		}

		// Wait for the intitialization of servers and clients to complete
		MPI_Barrier(MPI_COMM_WORLD);

		// Handle individual files by renaming the output once
		if (simulation_files == INDIVIDUAL) {
			// Each process should open its file
			sprintf(simulation_path, "%s-%03d", simulation_path, client_rank);
		}

		/*
		 * OPEN ------------------------------------------------------------------------------------
		 * Issue OPEN request to the forwarding layer
		 * -----------------------------------------------------------------------------------------
		 */

		// Create the request
		r = (struct request *) malloc(sizeof(struct request));

		r->operation = OPEN;
		strcpy(r->file_name, simulation_path);
		r->file_handle = -1;
		r->offset = 0;
		r->size = 1;

		// Issue the OPEN operation, that should wait for it to complete
		MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

		log_debug("waiting for the file handle...");
		
		// We need to wait for the file handle to continue
		MPI_Recv(&forwarding_fh, 1, MPI_INT, my_forwarding_server, TAG_HANDLE, MPI_COMM_WORLD, &status);

		log_debug("HANDLE received [id=%d] %d", request_id, forwarding_fh);

		MPI_Barrier(clients_comm);

		/*
		 * WRITE -----------------------------------------------------------------------------------
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

			if (simulation_files == SHARED) {
				if (simulation_spatiality == CONTIGUOUS) {
					// Offset computation is based on MPI-IO Test implementation
					r->offset = (floor(i / n) * ((simulation_request_size * n) * client_size)) + ((simulation_request_size * n) * client_rank) + (i * simulation_request_size);

				} else {
					// Offset computation is based on MPI-IO Test implementation
					r->offset = i * (client_size * simulation_request_size) + (client_rank * simulation_request_size);
				}
			} else {
				// Offset computation is based on MPI-IO Test implementation
				r->offset = i * simulation_request_size;
			}
			
			r->size = simulation_request_size;

			log_debug("[OP][%d] %d %d %ld %ld", world_rank, r->operation, r->file_handle, r->offset, r->size);

			// Issue the fake WRITE operation, that should wait for it to complete
			MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

			log_debug("waiting for the answer to come...");

			// We need to wait for the ACK so that the server is ready to receive our buffer and the request ID
			MPI_Recv(&request_id, 1, MPI_INT, my_forwarding_server, TAG_ACK, MPI_COMM_WORLD, &status);

			log_debug("ACK received [id=%d], sending buffer...", request_id);

			int send_status;

			// We need to wait for the WRITE request to return before issuing another request
			send_status = MPI_Send(buffer, r->size, MPI_CHAR, my_forwarding_server, request_id, MPI_COMM_WORLD);

			assert(send_status == MPI_SUCCESS);

			log_debug("sent WRITE data to %d with length %ld", my_forwarding_server, r->size);

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

			// Free the statistics related to each rank
			free(rank_elapsed);
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
		strcpy(r->file_name, simulation_path);
		r->file_handle = -1;
		r->offset = 0;
		r->size = 1;

		// Issue the CLOSE operation, that should wait for it to complete
		MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

		log_debug("waiting for the close ACK...");

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
		strcpy(r->file_name, simulation_path);
		r->file_handle = -1;
		r->offset = 0;
		r->size = 1;

		// Issue the OPEN operation, that should wait for it to complete
		MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

		log_debug("waiting for the file handle...");

		// We need to wait for the file handle to continue
		MPI_Recv(&forwarding_fh, 1, MPI_INT, my_forwarding_server, TAG_HANDLE, MPI_COMM_WORLD, &status);

		log_debug("HANDLE received [id=%d] %d", request_id, forwarding_fh);

		MPI_Barrier(clients_comm);
		
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
			
			if (simulation_files == SHARED) {
				if (simulation_spatiality == CONTIGUOUS) {
					// Offset computation is based on MPI-IO Test implementation
					r->offset = (floor(i / n) * ((simulation_request_size * n) * client_size)) + ((simulation_request_size * n) * client_rank) + (i * simulation_request_size);
				} else {
					// Offset computation is based on MPI-IO Test implementation
					r->offset = i * (client_size * simulation_request_size) + (client_rank * simulation_request_size);
				}
			} else {
				// Offset computation is based on MPI-IO Test implementation
				r->offset = i * simulation_request_size;
			}

			r->size = simulation_request_size;

			log_debug("[OP][%d] %d %d %ld %ld", world_rank, r->operation, r->file_handle, r->offset, r->size);

			// Issue the fake READ operation, that should wait for it to complete
			MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

			log_debug("waiting for the answer to come...");

			// We need to wait for the READ request to return before issuing another request
			MPI_Recv(buffer, r->size, MPI_CHAR, my_forwarding_server, TAG_BUFFER, MPI_COMM_WORLD, &status);

			int size = 0;

			// Get the size of the received message
			MPI_Get_count(&status, MPI_CHAR, &size);

			// Make sure we received all the buffer
			assert(r->size == size);

			// Discover the rank that sent us the message
			log_debug("received READ data from %d with length %d", status.MPI_SOURCE, size);
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
		strcpy(r->file_name, simulation_path);
		r->file_handle = -1;
		r->offset = 0;
		r->size = 1;

		// Issue the CLOSE operation, that should wait for it to complete
		MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

		log_debug("waiting for the close ACK...");

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

			// Free the statistics related to each rank
			free(rank_elapsed);
		}

		MPI_Barrier(clients_comm);

		// Free the request
		free(r);

		if (is_forwarding) {
			free(statistics);
		}

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

		log_info("rank %d is sending SHUTDOWN signal", world_rank);

		// Send shutdown message
		MPI_Send(buffer, EMPTY, MPI_CHAR, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD);
	}

	MPI_Barrier(MPI_COMM_WORLD);

	// Free the communicator used in the forwarding server
	if (forwarding_comm != MPI_COMM_NULL) {
		MPI_Comm_free(&forwarding_comm);
	}

	// Free the communicator used by the clients
	if (clients_comm != MPI_COMM_NULL) {
		MPI_Comm_free(&clients_comm);
	}

	MPI_Group_free(&forwarding_group); 
	MPI_Group_free(&clients_group); 
	MPI_Group_free(&world_group);

	MPI_Type_free(&request_datatype);

	// Free the buffer
	free(buffer);

	// Shut down MPI
	MPI_Finalize(); 

	return 0;
}

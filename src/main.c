#include "main.h"
#include "forge.h"
#include "scheduler.h"
#include "utils.h"
#include "log.h"
#include "thpool.h"

#include "server/listener.h"
#include "server/handler.h"
#include "server/dispatcher.h"

const int MAX_STRING = 100;
const int EMPTY = 0;

#ifdef DEBUG
struct ready_request *tmp;
#endif

FWD_LIST_HEAD(incoming_queue);
FWD_LIST_HEAD(ready_queue);

/**
 * Get the forwarding server the current node should communicate.
 * @return The rank that represents the forwarding server.
 */
int get_forwarding_server() {
    // We need to split the clients between the forwarding servers
    return (world_rank - simulation_forwarders) / ((world_size - simulation_forwarders) / simulation_forwarders);
}

static int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
    if (tok->type == JSMN_STRING && (int) strlen(s) == tok->end - tok->start && strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
        return 0;
    }

    return -1;
}

int main(int argc, char *argv[]) {
    int i, n, my_forwarding_server, ack, request_id, ret;
    int provided;

    struct timespec start_time, end_time;
    
    double elapsed;
    double *rank_elapsed;

    double elapsed_min, elapsed_q1, elapsed_mean, elapsed_q2, elapsed_q3, elapsed_max;

    MPI_Status status;

    if (argc != 3) {
        printf("\nFORGE - I/O Forwarding Emulator\n");
        printf("Usage: ./forge <configuration.json> <emulation.log>\n");

        return 0;
    }

    // Open log file
    FILE *log_file;

    log_file = fopen(argv[2], "a+");

    // Define the log file to be used
    log_set_fp(log_file);

    log_set_level(LOG_INFO);

    #ifdef DEBUG
    log_set_level(LOG_TRACE);
    #endif

    char *buffer = calloc(MAX_REQUEST_SIZE, sizeof(char));

    if (buffer == NULL) {
        log_error("ERROR: Unable to allocate the maximum memory size of %d bytes for requests ", MAX_REQUEST_SIZE);

        MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
    }

    provided = MPI_THREAD_SINGLE;

    // Start up MPI
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    
        // Make sure MPI has thread support
    if (provided != MPI_THREAD_MULTIPLE) {
        log_error("ERROR: the MPI library doesn't provide the required thread level");

        MPI_Abort(MPI_COMM_WORLD, ERROR_UNSUPPORTED);
    }

    // Get the number of processes
    MPI_Comm_size(MPI_COMM_WORLD, &world_size); 

    // Get my rank among all the processes
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank); 

    if (world_rank == 0) {
        log_info("I/O Forwarding Emulation [START]");
    }

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
    ret = fread(configuration, sizeof(char), bytes, json_file);

    if (ret < 0) {
        log_error("Failed to load JSON: %d", ret);
        
        MPI_Abort(MPI_COMM_WORLD, ERROR_FAILED_TO_LOAD_JSON);
    }

    // Closes the JSON configuration file
    fclose(json_file);

    jsmn_parser parser;
    jsmntok_t tokens[1024];

    jsmn_init(&parser);

    ret = jsmn_parse(&parser, configuration, strlen(configuration), tokens, sizeof(tokens) / sizeof(tokens[0]));
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
    char simulation_path[255] = "";

    char *simulation_base_path = "";
    char *simulation_files_name = "";
    char *simulation_spatiality_name = "";

    char simulation_map_file[255] = "";
    char simulation_time_file[255] = "";
    char simulation_stats_file[255] = "";

    int simulation_handlers = 0;
    int simulation_dispatchers = 0;

    int simulation_files = 0;
    int simulation_spatiality = 0;

    int simulation_validation = 0;

    unsigned long simulation_request_size = 0;
    unsigned long simulation_total_size = 0;
    unsigned long simulation_rank_size = 0;

    unsigned long b;

    char *parameter;

    // Loop over all keys of the root object
    for (i = 1; i < ret; i++) {
        if (jsoneq(configuration, &tokens[i], "forwarders") == 0) {
            parameter = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);
            simulation_forwarders = atoi(parameter);

            free(parameter);

            i++;
        } else if (jsoneq(configuration, &tokens[i], "handlers") == 0) {
            parameter = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);
            simulation_handlers = atoi(parameter);

            free(parameter);

            i++;
        } else if (jsoneq(configuration, &tokens[i], "dispatchers") == 0) {
            parameter = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);
            simulation_dispatchers = atoi(parameter);

            free(parameter);

            i++;
        } else if (jsoneq(configuration, &tokens[i], "path") == 0) {
            simulation_base_path = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);

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
            parameter = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);
            simulation_total_size = strtoul(parameter, NULL, 10);

            free(parameter);

            i++;
        } else if (jsoneq(configuration, &tokens[i], "request_size") == 0) {
            parameter = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);
            simulation_request_size = strtoul(parameter, NULL, 10);

            free(parameter);

            i++;
        } else if (jsoneq(configuration, &tokens[i], "validation") == 0) {
            parameter = strndup(configuration + tokens[i+1].start, tokens[i+1].end - tokens[i+1].start);
            simulation_validation = atoi(parameter);

            free(parameter);

            i++;

            if (simulation_validation < 0 || simulation_validation > 1) {
                // Handle unkown validation
                MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_VALIDATION);
            }
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
    if (simulation_handlers > FWD_MAX_HANDLER_THREADS) {
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

    safe_free(forwarding_ranks, "main_ranks:001");
    safe_free(client_ranks, "main_ranks:002");

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

    MPI_File fh = NULL;
    MPI_Status s;

    sprintf(simulation_map_file, "%s.map", base_file);
    sprintf(simulation_time_file, "%s.time", base_file);
    sprintf(simulation_stats_file, "%s.%d.stats", base_file, world_rank);

    char map[1024];
    float *forwading_map = NULL;

    if (world_rank == 0) {
        forwading_map = malloc(sizeof(float) * world_size);
    }
    
    MPI_Gather(&is_forwarding, 1, MPI_INT, forwading_map, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (world_rank == 0) {
        // Snapshot of the simulation configuration
        MPI_File_open(MPI_COMM_SELF, simulation_map_file, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
        
        for (i = 0; i < world_size; i++) {
            sprintf(map, "rank %d: %s\n", i, (forwading_map[i] ? "server" : "client"));

            // Write the subarray
            MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);
        }

        // Close the file
        MPI_File_close(&fh);
    }

    if (world_rank == 0) {
        // Free the map structure
        free(forwading_map);
    }

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

    pthread_rwlock_init(&requests_rwlock, NULL);
    pthread_rwlock_init(&handles_rwlock,NULL);

    if (is_forwarding) {
        global_id = 1000000;

        #ifdef PVFS
        pvfs_fh_id = 100;
        #endif

        shutdown = 0;
        shutdown_control = 0;

        // Initialize the list with incoming request to process
        init_fwd_list_head(&incoming_queue);

        // Initialize the list with the ready requests
        init_fwd_list_head(&ready_queue);

        // Initialize a thread pool to issue the requests and process the callbacks
        thread_pool = thpool_init(simulation_dispatchers);

        // Start the AGIOS scheduling library
        start_AGIOS(simulation_forwarders);

        // Create threads to list for requests
        pthread_t listener;
        pthread_t handler[FWD_MAX_HANDLER_THREADS];
        pthread_t dispatcher;

        #ifdef STATISTICS
        statistics = (struct forwarding_statistics *) malloc(sizeof(struct forwarding_statistics));

        statistics->open = 0;
        statistics->read = 0;
        statistics->write = 0;
        statistics->close = 0;

        statistics->read_size = 0;
        statistics->write_size = 0;
        #endif

        #ifdef PVFS
        ret = PVFS_util_init_defaults();

        if (ret < 0) {
            PVFS_perror("PVFS_util_init_defaults", ret);
        } else {
            log_info("listener: connected to the PVFS!");
        }

        memset(&credentials, 0, sizeof(PVFS_credentials));
        
        PVFS_util_gen_credentials(&credentials);
        #endif

        // Create threads to issue the requests
        pthread_create(&dispatcher, NULL, server_dispatcher, NULL);

        for (i = 0; i < simulation_handlers; i++) {
            pthread_create(&handler[i], NULL, server_handler, NULL);
        }

        // We have a single listen thread
        pthread_create(&listener, NULL, server_listener, NULL);

        // Wait for the intitialization of servers and clients to complete
        MPI_Barrier(MPI_COMM_WORLD);

        // Wait for processing threads to complete
        pthread_join(dispatcher, NULL);

        // Wait for listen threads to complete
        for (i = 0; i < simulation_handlers; i++) {
            pthread_join(handler[i], NULL);
        }

        // This will destroy the thread pool. If jobs are currently being executed, then it will wait for them to finish.
        thpool_destroy(thread_pool);

        pthread_join(listener, NULL);

        MPI_Barrier(forwarding_comm);

        #ifdef PVFS
        // Finalizes the PVFS connection
        PVFS_sys_finalize();
        #endif

        // Stops the AGIOS scheduling library
        stop_AGIOS();

        #ifdef STATISTICS
        MPI_File_open(MPI_COMM_SELF, simulation_stats_file, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

        char stats[1048576];
            
        // Write the statistics of each forwarding server
        sprintf(stats, "forwarder: %d\nopen: %ld\nread: %ld\nwrite: %ld\nclose: %ld\nread_size: %ld\nwrite_size: %ld\n\n",
            forwarding_rank,
            statistics->open,
            statistics->read,
            statistics->write,
            statistics->close,
            statistics->read_size,
            statistics->write_size
        );

        // Write the subarray
        MPI_File_write(fh, &stats, strlen(stats), MPI_CHAR, &s);

        // Close the file
        MPI_File_close(&fh);

        free(statistics);
        #endif
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
            buffer[b] = 'a' + (client_rank % 25);
        }

        if (client_rank == 0) {
            MPI_File_open(MPI_COMM_SELF, simulation_time_file, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);

            time_t t = time(NULL);
            struct tm *tm = localtime(&t);
            char timestamp[64];

            assert(strftime(timestamp, sizeof(timestamp), "%F | %T", tm));

            sprintf(map, "---------------------------\n I/O Forwarding Simulation\n---------------------------\n");
            MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);

            sprintf(map, " | %s | \n---------------------------\n forwarders: %13d\n clients:    %13d\n layout:     %13d\n spatiality: %13d\n request:    %13ld\n total:      %13ld\n---------------------------\n\n", timestamp, world_size - client_size, client_size, simulation_files, simulation_spatiality, simulation_request_size, simulation_total_size);
            MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);
        }

        // Wait for the intitialization of servers and clients to complete
        MPI_Barrier(MPI_COMM_WORLD);

        // Handle individual files by renaming the output once
        if (simulation_files == INDIVIDUAL) {
            // Each process should open its file
            sprintf(simulation_path, "%s-%03d", simulation_base_path, client_rank);
        } else {
            sprintf(simulation_path, "%s", simulation_base_path);
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

        free(r);

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

            log_trace("[OP][%d] %d %d %ld %ld", world_rank, r->operation, r->file_handle, r->offset, r->size);

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

            free(r);
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
            safe_free(rank_elapsed, "main_elapsed:001");
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

        free(r);

        MPI_Barrier(clients_comm);

        safe_free(buffer, "main::buffer");

        // Because of the validation we may have, we need to reset the buffer to avoid possible errors
        buffer = (char*) calloc(MAX_REQUEST_SIZE, sizeof(char));

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

        free(r);

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

            log_trace("[OP][%d] %d %d %ld %ld", world_rank, r->operation, r->file_handle, r->offset, r->size);
            
            // Issue the fake READ operation, that should wait for it to complete
            MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

            // We need to wait for the READ request to return before issuing another request
            MPI_Recv(buffer, r->size, MPI_CHAR, my_forwarding_server, TAG_BUFFER, MPI_COMM_WORLD, &status);
            
            int size = 0;

            // Get the size of the received message
            MPI_Get_count(&status, MPI_CHAR, &size);

            // Make sure we received all the buffer
            assert(r->size == size);

            // Discover the rank that sent us the message
            log_debug("received READ data from %d with length %d", status.MPI_SOURCE, size);

            int errors = 0;

            // Check if the data that we read is the data we wrote
            if (simulation_validation) {
                for (b = 0; b < r->size; b++) {
                    if (buffer[b] != 'a' + (client_rank % 25)) {
                        log_info("rank = %03d [%05ld] %c <> %c (%d)", client_rank, b, buffer[b], 'a' + (client_rank % 25), request_id);

                        errors++;
                    }
                }

                if (errors) {
                    log_error("data validation sent from rank %d is INCORRECT!", status.MPI_SOURCE);
                    
                    MPI_Abort(MPI_COMM_WORLD, ERROR_VALIDATION_FAILED);
                }
            }

            free(r);
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

        free(r);

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
            safe_free(rank_elapsed, "main_elapsed:002");
        }

        MPI_Barrier(clients_comm);

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

        log_trace("rank %d is sending SHUTDOWN signal to fwd %d", world_rank, my_forwarding_server);

        // Send shutdown message
        MPI_Send(buffer, EMPTY, MPI_CHAR, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD);

        MPI_Barrier(clients_comm);
        
        log_trace("all clients have completed");
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
    safe_free(buffer, "main:buffer");

    free(simulation_base_path);
    free(simulation_files_name);
    free(simulation_spatiality_name);

    if (world_rank == 0) {
        log_info("I/O Forwarding Emulation [COMPLETE]");
    }

    // Close log file
    fclose(log_file);

    // Shut down MPI
    MPI_Finalize();

    return 0;
}

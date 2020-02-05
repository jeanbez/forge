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

FWD_LIST_HEAD(phases);

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

int main(int argc, char *argv[]) {
    int i, n, my_forwarding_server, ack, request_id, ret;
    int provided;

    struct timespec start_time, end_time;
    
    double elapsed;
    double *rank_elapsed;
    double total_data;
    double rank_data;

    double elapsed_min, elapsed_q1, elapsed_mean, elapsed_q2, elapsed_q3, elapsed_max;

    MPI_Status status;

    if (argc != 3) {
        printf("\nFORGE - I/O Forwarding Emulator\n");
        printf("Usage: ./forge <configuration.json> <output path>\n");

        return 0;
    }

    struct stat sb;
    char *directory = argv[2];

    char simulation_log_file[255] = "";
    sprintf(simulation_log_file, "%s/emulation.log", directory);

    // Check if the path to store the logs exists
    if (!(stat(directory, &sb) == 0 && S_ISDIR(sb.st_mode))) {
        printf("\nFORGE - I/O Forwarding Emulator\n");
        printf("Invalid path to store logs!\n");

        return 0;
    }

    // Open log file
    FILE *log_file;

    log_file = fopen(simulation_log_file, "a+");

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

    cJSON *json_configuration = cJSON_Parse(configuration);
    cJSON *json_tag = NULL;
    cJSON *json_phase = NULL;
    cJSON *json_phases = NULL;
    cJSON *json_pattern = NULL;
    cJSON *json_patterns = NULL;

    // Initialize the list with incoming request to process
    init_fwd_list_head(&phases);

    #ifdef DEBUG
    if (world_rank == 0) {
        printf("%s\n", cJSON_Print(json_configuration));
    }
    #endif

    // Simulation configuration
    char simulation_path[300] = "";

    char *simulation_base_path = "";
    char *simulation_files_name = "";
    char *simulation_spatiality_name = "";

    char simulation_map_file[255] = "";
    char simulation_time_file[255] = "";
    #ifdef SIMULATION
    char simulation_stats_file[255] = "";
    #endif

    char *simulation_operation = "";

    int simulation_handlers = 0;
    int simulation_dispatchers = 0;

    int simulation_files = 0;
    int simulation_spatiality = 0;

    unsigned long simulation_request_size = 0;
    unsigned long simulation_total_size = 0;
    unsigned long simulation_rank_size = 0;

    double hit_stone_wall, rank_hit_stone_wall;

    unsigned long b;

    // Parse the configuration
    json_tag = cJSON_GetObjectItemCaseSensitive(json_configuration, "forwarders");
    
    if (cJSON_IsNumber(json_tag) && (json_tag->valueint > 0)) {
        simulation_forwarders = json_tag->valueint;
    }

    json_tag = cJSON_GetObjectItemCaseSensitive(json_configuration, "handlers");
    
    if (cJSON_IsNumber(json_tag) && (json_tag->valueint > 0)) {
        simulation_handlers = json_tag->valueint;
    }

    json_tag = cJSON_GetObjectItemCaseSensitive(json_configuration, "dispatchers");
    
    if (cJSON_IsNumber(json_tag) && (json_tag->valueint > 0)) {
        simulation_dispatchers = json_tag->valueint;
    }

    // Parse the I/O phases
    json_phases = cJSON_GetObjectItemCaseSensitive(json_configuration, "phases");

    cJSON_ArrayForEach(json_phase, json_phases) {
        struct phase *f;

        f = (struct phase *) malloc(sizeof(struct phase));
        f->total = 0;

        json_tag = cJSON_GetObjectItemCaseSensitive(json_phase, "repetitions");

        if (cJSON_IsNumber(json_tag) && (json_tag->valueint > 0)) {
            f->repetitions = json_tag->valueint;
        }

        // Parse the I/O phases
        json_patterns = cJSON_GetObjectItemCaseSensitive(json_phase, "patterns");

        cJSON_ArrayForEach(json_pattern, json_patterns) {
            struct pattern *p;

            p = (struct pattern *) malloc(sizeof(struct pattern));

            json_tag = cJSON_GetObjectItemCaseSensitive(json_pattern, "path");

            if (cJSON_IsString(json_tag) && (json_tag->valuestring != NULL)) {
                simulation_base_path = json_tag->valuestring;

                strcpy(p->path, simulation_base_path);
            }

            json_tag = cJSON_GetObjectItemCaseSensitive(json_pattern, "operation");

            if (cJSON_IsString(json_tag) && (json_tag->valuestring != NULL)) {
                simulation_operation = json_tag->valuestring;
                
                // Check for write operation
                if (strstr(simulation_operation, "open") != NULL) {
                    p->operation = OPEN;
                } else if (strstr(simulation_operation, "close") != NULL) {
                    p->operation = CLOSE;
                } else if (strstr(simulation_operation, "write") != NULL) {
                    p->operation = WRITE;
                } else if (strstr(simulation_operation, "read") != NULL) {
                    p->operation = READ;
                } else {
                    MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_OPERATION);
                }
            }

            json_tag = cJSON_GetObjectItemCaseSensitive(json_pattern, "number_of_files");

            if (cJSON_IsString(json_tag) && (json_tag->valuestring != NULL)) {
                simulation_files_name = json_tag->valuestring;
                
                // Check for supported spatialities
                if (strcmp("shared", simulation_files_name) == 0) {
                    simulation_files = SHARED;
                } else if (strcmp("individual", simulation_files_name) == 0) {
                    simulation_files = INDIVIDUAL;
                } else {
                    // Handle unkown patterns
                    MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_PATTERN);
                }

                p->number_of_files = simulation_files;
            }

            json_tag = cJSON_GetObjectItemCaseSensitive(json_pattern, "spatiality");

            if (cJSON_IsString(json_tag) && (json_tag->valuestring != NULL)) {
                simulation_spatiality_name = json_tag->valuestring;
                
                // Check for supported spatialities
                if (strcmp("contiguous", simulation_spatiality_name) == 0) {
                    simulation_spatiality = CONTIGUOUS;
                } else if (strcmp("strided", simulation_spatiality_name) == 0) {
                    simulation_spatiality = STRIDED;
                } else {
                    // Handle unkown patterns
                    MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_PATTERN);
                }

                p->spatiality = simulation_spatiality;
            }

            json_tag = cJSON_GetObjectItemCaseSensitive(json_pattern, "total_size");
        
            if (cJSON_IsNumber(json_tag) && (json_tag->valueint > 0)) {
                simulation_total_size = (unsigned long int) json_tag->valuedouble;

                p->total_size = simulation_total_size;
            }

            json_tag = cJSON_GetObjectItemCaseSensitive(json_pattern, "request_size");
        
            if (cJSON_IsNumber(json_tag) && (json_tag->valueint > 0)) {
                simulation_request_size = (unsigned long int) json_tag->valuedouble;

                p->request_size = simulation_request_size;
            }

            json_tag = cJSON_GetObjectItemCaseSensitive(json_pattern, "stone_wall");
        
            if (cJSON_IsNumber(json_tag) && (json_tag->valueint > 0)) {
                simulation_stone_wall = json_tag->valueint;

                p->stone_wall = simulation_stone_wall;
            }

            json_tag = cJSON_GetObjectItemCaseSensitive(json_pattern, "direct_io");
        
            if (cJSON_IsNumber(json_tag) && (json_tag->valueint > 0)) {
                simulation_direct_io = json_tag->valueint;

                p->direct_io = simulation_direct_io;
            }

            json_tag = cJSON_GetObjectItemCaseSensitive(json_pattern, "validation");
        
            if (cJSON_IsNumber(json_tag) && (json_tag->valueint > 0)) {
                simulation_validation = json_tag->valueint;

                if (simulation_validation < 0 || simulation_validation > 1) {
                    // Handle unkown validation
                    MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_VALIDATION);
                }

                p->validation = simulation_validation;
            }

            // Verify for an invalid pattern: individual files with 1D strided accesses
            if (simulation_files == INDIVIDUAL && simulation_spatiality == STRIDED) {
                log_error("invalid access pattern");

                MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_PATTERN);
            }

            f->patterns[f->total++] = p;
        }

        fwd_list_add_tail(&(f->list), &phases);
    }

    free(configuration);

    cJSON_Delete(json_configuration);

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

    sprintf(simulation_map_file, "%s/emulation.map", directory);
    sprintf(simulation_time_file, "%s/emulation.time", directory);
    #ifdef STATISTICS
    sprintf(simulation_stats_file, "%s/emulation.rank-%d.stats", directory, world_rank);
    #endif

    char map[1048576];
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
    pthread_rwlock_init(&handles_rwlock, NULL);

    set_page_size();

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
        int id, repetition, forwarding_fh;
        double start_stone_wall = 0;
        
        struct request *r = NULL;

        // Define the forwarding server we should interact with
        my_forwarding_server = get_forwarding_server();

        log_debug("RANK %d\tFORWARDER SERVER: %d", world_rank, my_forwarding_server);
        log_debug("sending message from process %d of %d!", world_rank, world_size);

        if (client_rank == 0) {
            time_t t = time(NULL);
            struct tm *tm = localtime(&t);
            char timestamp[64];

            assert(strftime(timestamp, sizeof(timestamp), "%F | %T", tm));

            sprintf(map + strlen(map), "----------------------------\n I/O Forwarding Simulation\n---------------------------\n");
            sprintf(map + strlen(map), " | %s | \n---------------------------\n forwarders: %d\n clients: %d\n---------------------------\n", timestamp, world_size - client_size, client_size);
        }

        // Wait for the intitialization of servers and clients to complete
        MPI_Barrier(MPI_COMM_WORLD);

        // Iterate over each phase and issue the proper requests
        struct phase *description;
        struct phase *tmp;

        struct pattern *simulation;

        log_trace("begin I/O phases from process %d of %d!", world_rank, world_size);

        fwd_list_for_each_entry_safe(description, tmp, &phases, list) {
            for (repetition = 0; repetition < description->repetitions; repetition++) {
                for (id = 0; id < description->total; id++) {
                    simulation = description->patterns[id];

                    // Fill the buffer with fake data to be written
                    for (b = 0; b < simulation->request_size; b++) {
                        buffer[b] = 'a' + (client_rank % 25);
                    }

                    // Handle individual files by renaming the output once
                    if (simulation->number_of_files == INDIVIDUAL) {
                        // Each process should open its file
                        sprintf(simulation_path, "%s-%03d", simulation->path, client_rank);
                    } else {
                        sprintf(simulation_path, "%s", simulation->path);
                    }

                    // Setup the simulation phase

                    if (simulation->operation == READ) {
                        if (client_rank == 0) {
                            sprintf(map + strlen(map), "\n---------------------------\n operation: %s\n layout: %s\n spatiality: %s\n odirect: %d\n stonewall: %d\n request: %ld KB\n total: %ld MB\n---------------------------\n", "read", simulation->number_of_files == INDIVIDUAL ? "individual" : "shared", simulation->spatiality == CONTIGUOUS ? "contiguous" : "strided", simulation->direct_io, simulation->stone_wall, simulation->request_size / 1024, simulation->total_size / 1024 / 1024);
                        }

                        start_stone_wall = 0;
                        hit_stone_wall = 0;
                        rank_hit_stone_wall = 0;

                        rank_data = 0.0;

                        // Define the size per process
                        simulation_rank_size = ceil(simulation->total_size / client_size);

                        n = simulation_rank_size / simulation->request_size;

                        /*
                         * READ ------------------------------------------------------------------------------------
                         * Issue READ requests to the forwarding layer
                         * -----------------------------------------------------------------------------------------
                         */

                        clock_gettime(CLOCK_MONOTONIC, &start_time);

                        start_stone_wall = MPI_Wtime();

                        for (i = 0; i < n; i++) {
                            // Create the request
                            r = (struct request *) malloc(sizeof(struct request));

                            r->operation = READ;
                            r->file_handle = forwarding_fh;
                            
                            if (simulation->number_of_files == SHARED) {
                                if (simulation->spatiality == CONTIGUOUS) {
                                    // Offset computation is based on MPI-IO Test implementation
                                    r->offset = (floor(i / n) * ((simulation->request_size * n) * client_size)) + ((simulation->request_size * n) * client_rank) + (i * simulation->request_size);
                                } else {
                                    // Offset computation is based on MPI-IO Test implementation
                                    r->offset = i * (client_size * simulation->request_size) + (client_rank * simulation->request_size);
                                }
                            } else {
                                // Offset computation is based on MPI-IO Test implementation
                                r->offset = i * simulation->request_size;
                            }

                            r->size = simulation->request_size;

                            rank_data += r->size;

                            log_trace("[OP][%d] %d %d %ld %ld", world_rank, r->operation, r->file_handle, r->offset, r->size);

                            // Issue the fake READ operation, that should wait for it to complete
                            MPI_Send(r, 1, request_datatype, my_forwarding_server, TAG_REQUEST, MPI_COMM_WORLD); 

                            // Received the request ID
                            MPI_Recv(&request_id, 1, MPI_INT, my_forwarding_server, TAG_ACK, MPI_COMM_WORLD, &status);

                            // We need to wait for the READ request to return before issuing another request
                            MPI_Recv(buffer, r->size, MPI_CHAR, my_forwarding_server, request_id, MPI_COMM_WORLD, &status);
                            
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

                            // Check for stonewall time (zero will disable this feature)
                            if (simulation->stone_wall > 0 && (MPI_Wtime() - start_stone_wall) > simulation->stone_wall) {
                                rank_hit_stone_wall = MPI_Wtime() - start_stone_wall;

                                break;
                            }
                        }

                        clock_gettime(CLOCK_MONOTONIC, &end_time);

                        // Execution time in nano seconds and convert it to seconds
                        elapsed = ((end_time.tv_nsec - start_time.tv_nsec) + ((end_time.tv_sec - start_time.tv_sec)*1000000000L)) / 1000000000.0;

                        MPI_Reduce(&rank_hit_stone_wall, &hit_stone_wall, 1, MPI_DOUBLE, MPI_MAX, 0, clients_comm);

                        if (client_rank == 0 && hit_stone_wall > 0) {
                            log_warn("hit stone wall at %lf seconds!", hit_stone_wall);
                        }

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

                        MPI_Reduce(&rank_data, &total_data, 1, MPI_DOUBLE, MPI_SUM, 0, clients_comm);

                        if (client_rank == 0) {
                            // Snapshot of the simulation configuration
                            #ifdef STATISTICS
                            for (i = 0; i < client_size; i++) {
                                sprintf(map + strlen(map), " rank %03d: %.5lf s\n", i, rank_elapsed[i]);
                            }

                            sprintf(map + strlen(map), "----------------------------\n");
                            #endif

                            elapsed_min = gsl_stats_min(rank_elapsed, 1, client_size);
                            elapsed_mean = gsl_stats_mean(rank_elapsed, 1, client_size);
                            elapsed_max = gsl_stats_max(rank_elapsed, 1, client_size);

                            gsl_sort(rank_elapsed, 1, client_size);

                            elapsed_q1 = gsl_stats_quantile_from_sorted_data(rank_elapsed, 1, client_size, 0.25);
                            elapsed_q2 = gsl_stats_median_from_sorted_data(rank_elapsed, 1, client_size);
                            elapsed_q3 = gsl_stats_quantile_from_sorted_data(rank_elapsed, 1, client_size, 0.75);

                            sprintf(map + strlen(map), " min: %.5lf s\n Q1: %.5lf s\n Q2: %.5lf s\n Q3: %.5lf s\n max: %.5lf s\n mean: %.5lf s\n", elapsed_min, elapsed_q1, elapsed_q2, elapsed_q3, elapsed_max, elapsed_mean);
                            sprintf(map + strlen(map), "----------------------------\n");
                            sprintf(map + strlen(map), " data: %.5lf MB\n", total_data / 1024.0 / 1024);
                            sprintf(map + strlen(map), " bandwidth: %.5lf MB/s\n", (total_data / 1024.0 / 1024) / elapsed_max);
                            sprintf(map + strlen(map), "----------------------------\n");
                            
                            // Free the statistics related to each rank
                            safe_free(rank_elapsed, "main_elapsed:002");
                        }
                    } else if (simulation->operation == WRITE) {
                        if (client_rank == 0) {
                            sprintf(map + strlen(map), "\n---------------------------\n operation: %s\n layout: %s\n spatiality: %s\n odirect: %d\n stonewall: %d\n request: %ld KB\n total: %ld MB\n---------------------------\n", "write", simulation->number_of_files == INDIVIDUAL ? "individual" : "shared", simulation->spatiality == CONTIGUOUS ? "contiguous" : "strided", simulation->direct_io, simulation->stone_wall, simulation->request_size / 1024, simulation->total_size / 1024 / 1024);
                        }

                        start_stone_wall = 0;
                        hit_stone_wall = 0;
                        rank_hit_stone_wall = 0;

                        rank_data = 0.0;

                        // Define the size per process
                        simulation_rank_size = ceil(simulation->total_size / client_size);

                        n = simulation_rank_size / simulation->request_size;

                        /*
                         * WRITE -----------------------------------------------------------------------------------
                         * Issue WRITE requests to the forwarding layer
                         * -----------------------------------------------------------------------------------------
                         */

                        clock_gettime(CLOCK_MONOTONIC, &start_time);

                        start_stone_wall = MPI_Wtime();

                        for (i = 0; i < n; i++) {
                            // Create the request
                            r = (struct request *) malloc(sizeof(struct request));

                            r->operation = WRITE;
                            r->file_handle = forwarding_fh;

                            if (simulation->number_of_files == SHARED) {
                                if (simulation->spatiality == CONTIGUOUS) {
                                    // Offset computation is based on MPI-IO Test implementation
                                    r->offset = (floor(i / n) * ((simulation->request_size * n) * client_size)) + ((simulation->request_size * n) * client_rank) + (i * simulation->request_size);

                                } else {
                                    // Offset computation is based on MPI-IO Test implementation
                                    r->offset = i * (client_size * simulation->request_size) + (client_rank * simulation->request_size);
                                }
                            } else {
                                // Offset computation is based on MPI-IO Test implementation
                                r->offset = i * simulation->request_size;
                            }
                            
                            r->size = simulation->request_size;

                            rank_data += r->size;

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

                            // Check for stonewall time (zero will disable this feature)
                            if (simulation->stone_wall > 0 && (MPI_Wtime() - start_stone_wall) > simulation->stone_wall) {
                                rank_hit_stone_wall = MPI_Wtime() - start_stone_wall;

                                break;
                            }
                        }

                        clock_gettime(CLOCK_MONOTONIC, &end_time);

                        // Execution time in nano seconds and convert it to seconds
                        elapsed = ((end_time.tv_nsec - start_time.tv_nsec) + ((end_time.tv_sec - start_time.tv_sec)*1000000000L)) / 1000000000.0;
                        
                        MPI_Reduce(&rank_hit_stone_wall, &hit_stone_wall, 1, MPI_DOUBLE, MPI_MAX, 0, clients_comm);

                        if (client_rank == 0 && hit_stone_wall > 0) {
                            log_warn("hit stone wall at %lf seconds!", hit_stone_wall);
                        }

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

                        MPI_Reduce(&rank_data, &total_data, 1, MPI_DOUBLE, MPI_SUM, 0, clients_comm);

                        if (client_rank == 0) {
                            #ifdef STATISTICS
                            // Snapshot of the simulation configuration
                            for (i = 0; i < client_size; i++) {
                                sprintf(map + strlen(map), " rank %03d: %.5lf s\n", i, rank_elapsed[i]);
                            }

                            sprintf(map + strlen(map), "----------------------------\n");
                            #endif

                            elapsed_min = gsl_stats_min(rank_elapsed, 1, client_size);
                            elapsed_mean = gsl_stats_mean(rank_elapsed, 1, client_size);
                            elapsed_max = gsl_stats_max(rank_elapsed, 1, client_size);

                            gsl_sort(rank_elapsed, 1, client_size);

                            elapsed_q1 = gsl_stats_quantile_from_sorted_data(rank_elapsed, 1, client_size, 0.25);
                            elapsed_q2 = gsl_stats_median_from_sorted_data(rank_elapsed, 1, client_size);
                            elapsed_q3 = gsl_stats_quantile_from_sorted_data(rank_elapsed, 1, client_size, 0.75);

                            sprintf(map + strlen(map), " min: %.5lf s\n Q1: %.5lf s\n Q2: %.5lf s\n Q3: %.5lf s\n max: %.5lf s\n mean: %.5lf s\n", elapsed_min, elapsed_q1, elapsed_q2, elapsed_q3, elapsed_max, elapsed_mean);
                            sprintf(map + strlen(map), "----------------------------\n");
                            sprintf(map + strlen(map), " data: %.5lf MB\n", total_data / 1024.0 / 1024);
                            sprintf(map + strlen(map), " bandwidth: %.5lf MB/s\n", (total_data / 1024.0 / 1024) / elapsed_max);
                            sprintf(map + strlen(map), "----------------------------\n");
                            
                            // Free the statistics related to each rank
                            safe_free(rank_elapsed, "main_elapsed:001");
                        }
                        
                        MPI_Barrier(clients_comm);
                    } else if (simulation->operation == OPEN) {
                        if (client_rank == 0) {
                            sprintf(map + strlen(map), "\n---------------------------\n operation: %s\n layout: %s\n---------------------------\n", "open", simulation->number_of_files == INDIVIDUAL ? "individual" : "shared");
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
                    } else if (simulation->operation == CLOSE) {
                        if (client_rank == 0) {
                            sprintf(map + strlen(map), "\n---------------------------\n operation: %s\n layout: %s\n---------------------------\n", "close", simulation->number_of_files == INDIVIDUAL ? "individual" : "shared");
                        }

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
                        free(buffer);

                        MPI_Barrier(clients_comm);

                        // Because of the validation we may have, we need to reset the buffer to avoid possible errors
                        buffer = (char*) calloc(MAX_REQUEST_SIZE, sizeof(char));
                    } else {
                        // TODO: handle error
                    }
                }

                MPI_Barrier(clients_comm);
            }

            MPI_Barrier(clients_comm);       
        }

        MPI_Barrier(clients_comm);

        /*
         * SHUTDOWN---------------------------------------------------------------------------------
         * Issue SHUTDOWN requests to the forwarding layer to finish the experiment
         * -----------------------------------------------------------------------------------------
         */

        if (client_rank == 0) {
            MPI_File_open(MPI_COMM_SELF, simulation_time_file, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
            MPI_File_write(fh, &map, strlen(map), MPI_CHAR, &s);
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

    if (world_rank == 0) {
        log_info("I/O Forwarding Emulation [COMPLETE]");
    }

    // Close log file
    fclose(log_file);

    // Shut down MPI
    MPI_Finalize();

    return 0;
}

#include "server/listener.h"

/**
 * Dedicated thread to handle incoming requests.
 */
void *server_listener(void *p) {
    int i = 0, flag = 0;

    MPI_Datatype request_datatype;
    int block_lengths[5] = {255, 1, 1, 1, 1};
    MPI_Datatype type[5] = {MPI_CHAR, MPI_INT, MPI_INT, MPI_UNSIGNED_LONG, MPI_UNSIGNED_LONG};
    MPI_Aint displacement[5];

    struct request req;

    MPI_Request request;
    MPI_Status status;

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

    log_debug("LISTEN THREAD %ld", pthread_self());

    // Listen for incoming requests
    while (1) {
        // Receive the message
        MPI_Irecv(&req, 1, request_datatype, MPI_ANY_SOURCE, TAG_REQUEST, MPI_COMM_WORLD, &request);

        MPI_Test(&request, &flag, &status);

        while (!flag) {
            // If all the nodes requested a shutdown, we can proceed
            if (shutdown_control) {
                log_debug("SHUTDOWN: listen thread %ld", pthread_self());

                // We need to signal the processing thread to proceed and check for shutdown
                pthread_cond_broadcast(&ready_queue_signal);
                
                // We need to cancel the MPI_Irecv
                MPI_Cancel(&request);

                MPI_Type_free(&request_datatype);

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

            if ((world_size - simulation_forwarders) / simulation_forwarders == shutdown) {
                shutdown_control = 1;

                pthread_cond_broadcast(&incoming_queue_signal);
                pthread_cond_broadcast(&ready_queue_signal);
            }
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

        // Place the request in the incoming queue to be handled by one of the threads
        pthread_mutex_lock(&incoming_queue_mutex);
        fwd_list_add_tail(&(r->list), &incoming_queue);
        pthread_mutex_unlock(&incoming_queue_mutex);

        // Signal the condition variable that new data is available in the queue
        pthread_cond_signal(&incoming_queue_signal);
    }

    return NULL;
}
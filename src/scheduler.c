#include "scheduler.h"

/**
 * AGIOS callback function to put a single request in the ready queue.
 * @param id The request ID.
 */
void callback(unsigned long long int id) {
    log_trace("AGIOS_CALLBACK: %ld", id);

    #ifdef DEBUG
    struct ready_request *tmp;
    #endif

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

    pthread_mutex_unlock(&ready_queue_mutex);

    // Signal the condition variable that new data is available in the queue
    pthread_cond_signal(&ready_queue_signal);
}

/**
 * AGIOS callback function to put multiple requests in the ready queue.
 * @param *ids The list of requests.
 * @param total The total number of requests.
 */
void callback_aggregated(unsigned long long int *ids, int total) {
    int i;

    #ifdef DEBUG
    struct ready_request *tmp;
    #endif

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

    pthread_mutex_unlock(&ready_queue_mutex);

    // Signal the condition variable that new data is available in the queue
    pthread_cond_signal(&ready_queue_signal);
}

/**
 * Stop the AGIOS scheduling library.
 */
void stop_AGIOS() {
    log_debug("stopping AGIOS scheduling library");

    agios_exit();
}

/**
 * Start the AGIOS scheduling library and define the callbacks.
 */
void start_AGIOS(int simulation_forwarders) {
    agios_client.process_request = (void *) callback;
    agios_client.process_requests = (void *) callback_aggregated;

    // Check if AGIOS was successfully inicialized
    if (agios_init(&agios_client, AGIOS_CONFIGURATION, simulation_forwarders) != 0) {
        log_debug("Unable to initialize AGIOS scheduling library");

        stop_AGIOS();

        MPI_Abort(MPI_COMM_WORLD, ERROR_AGIOS_INITIALIZATION);
    }
}

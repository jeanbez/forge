#include "server/dispatcher.h"

#ifdef PVFS
#include "dispatcher/orangefs.h"
#else
#include "dispatcher/posix.h"
#endif

/**
 * Dispatch the request to the file system once they have been scheduled.
 */
void *server_dispatcher(void *p) {
    int request_id, next_request_id;

    struct forwarding_request *r;
    struct forwarding_request *next_r;

    struct ready_request *ready_r;
    struct ready_request *next_ready_r;

    struct ready_request *tmp;

    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    timeout.tv_sec += TIMEOUT;

    while (1) {
        // Start by locking the queue mutex
        pthread_mutex_lock(&ready_queue_mutex);

        // While the queue is empty wait for the condition variable to be signalled
        while (fwd_list_empty(&ready_queue)) {
            // Check for shutdown signal
            if (shutdown_control) {
                // Unlock the queue mutex to allow other threads to complete
                pthread_mutex_unlock(&ready_queue_mutex);

                log_debug("SHUTDOWN: dispatcher %d thread %ld", world_rank, pthread_self());

                // Signal the condition variable of the handlers to handle the shutdown
                pthread_cond_signal(&incoming_queue_signal);

                return NULL;
            }

            // This call unlocks the mutex when called and relocks it before returning!
            pthread_cond_wait(&ready_queue_signal, &ready_queue_mutex); //, &timeout);
        }

        // There must be data in the ready queue, so we can get it
        ready_r = fwd_list_entry(ready_queue.next, struct ready_request, list);

        // Fetch the request ID
        request_id = ready_r->id;

        log_debug("[P-%ld] ---> ISSUE: %d", pthread_self(), request_id);

        // Remove it from the queue
        fwd_list_del(&ready_r->list);

        safe_free(ready_r, "server_dispatcher::001");

        // Unlock the queue mutex
        pthread_mutex_unlock(&ready_queue_mutex);

        // Process the request
        #ifdef DEBUG
        // Discover the rank that sent us the message
        pthread_rwlock_rdlock(&requests_rwlock);
        log_debug("[P-%ld] Pending requests: %u", pthread_self(), HASH_COUNT(requests));
        pthread_rwlock_unlock(&requests_rwlock);
        
        log_debug("[P-%ld] Request ID: %u", pthread_self(), request_id);
        #endif

        // Get the request from the hash and remove it from there
        pthread_rwlock_rdlock(&requests_rwlock);
        HASH_FIND_INT(requests, &request_id, r);

        if (r == NULL) {
            log_error("1. unable to find the request id: %ld", request_id);
            MPI_Abort(MPI_COMM_WORLD, ERROR_INVALID_REQUEST_ID);
        }
        pthread_rwlock_unlock(&requests_rwlock);
        
        log_trace("[XX][%d] %d %d %ld %ld", r->id, r->operation, r->file_handle, r->offset, r->size);

        // Create the aggregated request
        struct aggregated_request *aggregated;
        aggregated = (struct aggregated_request *) malloc(sizeof(struct aggregated_request));
        
        // Set the file handle for the aggregated request
        aggregated->file_handle = r->file_handle;
        
        #ifdef PVFS
        if (r->operation == WRITE) {
            aggregated->operation = PVFS_IO_WRITE;
        }

        if (r->operation == READ) {
            aggregated->operation = PVFS_IO_READ;
        }
        #else
        aggregated->operation = r->operation;
        #endif

        aggregated->count = 0;

        aggregated->requests[aggregated->count] = *r;
        aggregated->count++;

        // Start by locking the queue mutex
        pthread_mutex_lock(&ready_queue_mutex);

        fwd_list_for_each_entry_safe(next_ready_r, tmp, &ready_queue, list) {
            if (aggregated->count >= MAX_BATCH_SIZE) {
                break;
            }

            // Fetch the next request ID
            next_request_id = next_ready_r->id;

            // Get the request
            pthread_rwlock_rdlock(&requests_rwlock);
            HASH_FIND_INT(requests, &next_request_id, next_r);
            
            if (next_r == NULL) {
                log_error("2. unable to find the request");
            }
            pthread_rwlock_unlock(&requests_rwlock);
            
            // Check if it is for the same filehandle
            if (r->operation == next_r->operation && r->file_handle == next_r->file_handle) {
                log_debug("---> AGGREGATE (%ld + %ld) offsets = [%ld, %ld]", r->id, next_r->id, r->offset, next_r->offset);

                // Remove the request form the list
                fwd_list_del(&next_ready_r->list);

                safe_free(next_ready_r, "server_dispatcher::005");

                // Make sure we know which requests we aggregated so that we can reply to their clients
                aggregated->requests[aggregated->count] = *next_r;
                aggregated->count++;
            } else {
                // Requests are not of the same operation nor to the same filehandle
                break;
            }    
        }

        // Unlock the queue mutex
        pthread_mutex_unlock(&ready_queue_mutex);
                
        // Issue the large aggregated request
        thpool_add_work(thread_pool, (void*)dispatch_operation, aggregated);

        // We need to signal the processing thread to proceed and check for shutdown
        pthread_cond_signal(&ready_queue_signal);
    }

    return NULL;
}


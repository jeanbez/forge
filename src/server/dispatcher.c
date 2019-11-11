#include "server/dispatcher.h"

#ifdef PVFS
#include "dispatcher/orangefs.h"
#else
#include "dispatcher/posix.h"
#endif

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

/**
 * Dispatch the request to the file system once they have been scheduled.
 */
void *server_dispatcher(void *p) {
    int request_id, next_request_id, last_request, max_check_aggregated;

    struct forwarding_request *r;
    struct forwarding_request *next_r;

    struct ready_request *ready_r;
    struct ready_request *next_ready_r;

    struct ready_request *tmp;

    char *aggregated_write_buffer;

    int aggregated_count = 0;
    unsigned long int aggregated[MAX_BATCH_SIZE];
    unsigned long int aggregated_size = 0;
    unsigned long int aggregated_offset = 0;

    #ifdef PVFS
    int32_t aggregated_sizes[MAX_BATCH_SIZE];
    int64_t aggregated_offsets[MAX_BATCH_SIZE];
    #endif

    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    timeout.tv_sec += TIMEOUT;

    pqueue_t *pq;

    aggregated_write_buffer = calloc(MAX_BUFFER_SIZE, sizeof(char));

    // Initialize the priority queue for the dispatcher
    pq = pqueue_init(MAX_QUEUE_ELEMENTS, compare_priority, get_priority, set_priority, get_position, set_position);

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

                pqueue_free(pq);

                // Only at the end we can remove the buffer
                free(aggregated_write_buffer);

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

        // Issue the request to the filesystem
        if (r->operation == WRITE) {
            // Start by locking the queue mutex
            pthread_mutex_lock(&ready_queue_mutex);

            // Insert the first request in the priority queue
            node_t *pq_item = malloc(sizeof(node_t));

            pq_item->priority = -r->offset;
            pq_item->value = r->id;

            pqueue_insert(pq, pq_item);

            max_check_aggregated = 0;
            
            fwd_list_for_each_entry_safe(next_ready_r, tmp, &ready_queue, list) {
                if (max_check_aggregated++ >= MAX_BATCH_SIZE) {
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
                    node_t *pq_item = malloc(sizeof(node_t));

                    pq_item->priority = -next_r->offset;
                    pq_item->value = next_r->id;
                    
                    pqueue_insert(pq, pq_item);
                    
                    // Remove the request form the list
                    fwd_list_del(&next_ready_r->list);

                    safe_free(next_ready_r, "server_dispatcher::005");
                } else {
                    // Requests are not contiguous to the same filehandle
                    break;
                }    
            }

            // Unlock the queue mutex
            pthread_mutex_unlock(&ready_queue_mutex);
            
            #ifdef DEBUG
            pqueue_print(pq, stdout, print_node);
            #endif

            log_debug("items in possible aggregation queue: %d", pqueue_size(pq));

            node_t *next;

            // Make all the possible aggregations

            // Get the first request form the priority queue to serve as a base for aggregations
            next = pqueue_pop(pq);

            // We need to get the request to be our start
            pthread_rwlock_rdlock(&requests_rwlock);
            log_trace("next->priority = %d, next->value = %d", next->priority, next->value);
            HASH_FIND_INT(requests, &next->value, r);

            if (r == NULL) {
                log_error("3. unable to find the request");
            };
            pthread_rwlock_unlock(&requests_rwlock);
            
            log_trace("r->id = %ld, r->offset = %ld, r->size = %ld", r->id, r->offset, r->size);

            free(next);

            // We need to check if we have contiguous request to the same file handle, to aggregate them
            aggregated_count = 0;
            aggregated[aggregated_count] = r->id;
            #ifdef PVFS
            aggregated_sizes[aggregated_count] = r->size;
            aggregated_offsets[aggregated_count] = r->offset;
            #endif
            aggregated_size = r->size;
            aggregated_offset = 0;
            aggregated_count++;

            // Copy the write request to the buffer
            memcpy(&aggregated_write_buffer[aggregated_offset], r->buffer, r->size);
            aggregated_offset += r->size;
            
            last_request = 0;

            // We iterate the aggreation queue (which is local to every process) and merge the requests
            while (1) {
                next = pqueue_pop(pq);

                if (next != NULL) {
                    // We need to get the next request according to the priority
                    pthread_rwlock_rdlock(&requests_rwlock);
                    HASH_FIND_INT(requests, &next->value, next_r);

                    if (next_r == NULL) {
                        log_error("4. unable to find the request %lld", next->value);
                    };
                    pthread_rwlock_unlock(&requests_rwlock);

                    free(next);
                    
                    // See if we can actually aggregate them
                    log_trace("> %lld == %lld + %lld", next_r->offset, r->offset, aggregated_size);
                } else {
                    last_request = 1;
                }
                
                if (next != NULL && aggregated_count < MAX_BATCH_SIZE && next_r->offset == r->offset + aggregated_size) {
                    // Determine the aggregated request size
                    aggregated_size += next_r->size;

                    log_debug("---> AGGREGATE (%ld + %ld) [size = %ld, offset = %ld] aggregated_size = %ld!", r->id, next_r->id, next_r->size, next_r->offset, aggregated_size);
                    
                    // Make sure we know which requests we aggregated so that we can reply to their clients
                    aggregated[aggregated_count] = next_r->id;
                    #ifdef PVFS
                    aggregated_sizes[aggregated_count] = next_r->size;
                    aggregated_offsets[aggregated_count] = next_r->offset;
                    #endif
                    aggregated_count++;

                    // Copy the write request to the buffer
                    memcpy(&aggregated_write_buffer[aggregated_offset], next_r->buffer, next_r->size);
                    aggregated_offset += next_r->size;
                } else {
                    // We need to make the request to the file system and reply to the client
                    log_debug("aggregated_count = %ld", aggregated_count);

                    struct aggregated_request *agg;
                    agg = (struct aggregated_request *) malloc(sizeof(struct aggregated_request));

                    memcpy(agg->ids, aggregated, sizeof(aggregated));

                    agg->r = r;
                    agg->count = aggregated_count;
                    agg->size = aggregated_size;

                    // Allocates the buffer for the aggregated request with the precise size to free the temporary aggregate_buffer for future use
                    // TODO: try to improve this as it may force us to memcpy twice, but it believe it is better than realloc and memcpy every time we need to aggregate
                    agg->buffer = calloc(aggregated_size, sizeof(char));

                    memcpy(agg->buffer, aggregated_write_buffer, agg->size);
                    
                    // Issue the large aggregated request
                    thpool_add_work(thread_pool, (void*)dispatch_write, agg);

                    if (last_request) {
                        break;
                    }

                    r = next_r;

                    // Reset the aggregation count and add the pending request
                    aggregated_count = 0;
                    aggregated[aggregated_count] = r->id;
                    #ifdef PVFS
                    aggregated_sizes[aggregated_count] = r->size;
                    aggregated_offsets[aggregated_count] = r->offset;
                    #endif
                    aggregated_size = r->size;
                    aggregated_offset = 0;
                    aggregated_count++;

                    // Copy the write request to the buffer
                    memcpy(&aggregated_write_buffer[aggregated_offset], r->buffer, r->size);
                    aggregated_offset += r->size;
                }
            }
        } else if (r->operation == READ) {
            // Start by locking the queue mutex
            pthread_mutex_lock(&ready_queue_mutex);

            // Insert the first request in the priority queue
            node_t *pq_item = malloc(sizeof(node_t));

            pq_item->priority = -r->offset;
            pq_item->value = r->id;

            pqueue_insert(pq, pq_item);

            max_check_aggregated = 0;
            
            fwd_list_for_each_entry_safe(next_ready_r, tmp, &ready_queue, list) {
                if (max_check_aggregated++ >= MAX_BATCH_SIZE) {
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
                    node_t *pq_item = malloc(sizeof(node_t));

                    pq_item->priority = -next_r->offset;
                    pq_item->value = next_r->id;
                    
                    pqueue_insert(pq, pq_item);
                    
                    // Remove the request form the list
                    fwd_list_del(&next_ready_r->list);

                    safe_free(next_ready_r, "server_dispatcher::005");
                } else {
                    // Requests are not contiguous to the same filehandle
                    break;
                }    
            }

            // Unlock the queue mutex
            pthread_mutex_unlock(&ready_queue_mutex);
            
            #ifdef DEBUG
            pqueue_print(pq, stdout, print_node);
            #endif

            log_debug("items in possible aggregation queue: %d", pqueue_size(pq));

            node_t *next;

            // Make all the possible aggregations

            // Get the first request form the priority queue to serve as a base for aggregations
            next = pqueue_pop(pq);

            // We need to get the request to be our start
            pthread_rwlock_rdlock(&requests_rwlock);
            log_trace("next->priority = %d, next->value = %d", next->priority, next->value);
            HASH_FIND_INT(requests, &next->value, r);

            if (r == NULL) {
                log_error("3. unable to find the request");
            };
            pthread_rwlock_unlock(&requests_rwlock);
            
            log_trace("r->id = %ld, r->offset = %ld, r->size = %ld", r->id, r->offset, r->size);

            free(next);

            // We need to check if we have contiguous request to the same file handle, to aggregate them
            aggregated_count = 0;
            aggregated[aggregated_count] = r->id;
            #ifdef PVFS
            aggregated_sizes[aggregated_count] = r->size;
            aggregated_offsets[aggregated_count] = r->offset;
            #endif
            aggregated_size = r->size;
            aggregated_count++;

            last_request = 0;

            // We iterate the aggreation queue (which is local to every process) and merge the requests
            while (1) {
                next = pqueue_pop(pq);

                if (next != NULL) {
                    // We need to get the next request according to the priority
                    pthread_rwlock_rdlock(&requests_rwlock);
                    HASH_FIND_INT(requests, &next->value, next_r);

                    if (next_r == NULL) {
                        log_error("4. unable to find the request %lld", next->value);
                    };
                    pthread_rwlock_unlock(&requests_rwlock);
                    
                    free(next);
                    
                    // See if we can actually aggregate them
                    log_trace("> %lld == %lld + %lld", next_r->offset, r->offset, aggregated_size);
                } else {
                    last_request = 1;
                }
                
                if (next != NULL && aggregated_count < MAX_BATCH_SIZE && next_r->offset == r->offset + aggregated_size) {
                    // Determine the aggregated request size
                    aggregated_size += next_r->size;

                    log_debug("---> AGGREGATE (%ld + %ld) [size = %ld, offset = %ld] aggregated_size = %ld!", r->id, next_r->id, next_r->size, next_r->offset, aggregated_size);
                    
                    // Make sure we know which requests we aggregated so that we can reply to their clients
                    aggregated[aggregated_count] = next_r->id;
                    #ifdef PVFS
                    aggregated_sizes[aggregated_count] = next_r->size;
                    aggregated_offsets[aggregated_count] = next_r->offset;
                    #endif
                    aggregated_count++;
                } else {
                    // We need to make the request to the file system and reply to the client
                    log_debug("aggregated_count = %ld", aggregated_count);

                    struct aggregated_request *agg;
                    agg = (struct aggregated_request *) malloc(sizeof(struct aggregated_request));

                    memcpy(agg->ids, aggregated, sizeof(aggregated));

                    agg->r = r;
                    agg->count = aggregated_count;
                    agg->size = aggregated_size;
                    agg->buffer = calloc(aggregated_size, sizeof(char));
                    
                    // Issue the large aggregated request
                    thpool_add_work(thread_pool, (void*)dispatch_read, agg);

                    if (last_request) {
                        break;
                    }

                    r = next_r;

                    // Reset the aggregation count and add the pending request
                    aggregated_count = 0;
                    aggregated[aggregated_count] = r->id;
                    #ifdef PVFS
                    aggregated_sizes[aggregated_count] = r->size;
                    aggregated_offsets[aggregated_count] = r->offset;
                    #endif
                    aggregated_size = r->size;
                    aggregated_count++;
                }
            }
        }

        // We need to signal the processing thread to proceed and check for shutdown
        pthread_cond_signal(&ready_queue_signal);
    }

    return NULL;
}


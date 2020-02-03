#include "server/handler.h"
#include "handler/read.h"
#include "handler/write.h"

/**
 * Handles the incoming requests, processing open and write calls, and scheduling read and write requests to AGIOS.
 */
void *server_handler(void *p) {
    int ack;

    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    timeout.tv_sec += TIMEOUT;
    

    struct forwarding_request *r;

    while (1) {
        // Start by locking the incoming queue mutex
        pthread_mutex_lock(&incoming_queue_mutex);

        // While the queue is empty wait for the condition variable to be signalled
        while (fwd_list_empty(&incoming_queue)) {
            // Check for shutdown signal
            if (shutdown_control) {
                // Unlock the queue mutex to allow other threads to complete
                pthread_mutex_unlock(&incoming_queue_mutex);

                pthread_cond_broadcast(&incoming_queue_signal);

                log_debug("SHUTDOWN: handler thread %ld", pthread_self());

                return NULL;
            }

            // This call unlocks the mutex when called and relocks it before returning!
            pthread_cond_wait(&incoming_queue_signal, &incoming_queue_mutex); //, &timeout);
        }

        // There must be data in the ready queue, so we can get it
        r = fwd_list_entry(incoming_queue.next, struct forwarding_request, list);

        // Remove it from the queue
        fwd_list_del(&r->list);

        // Unlock the queue mutex
        pthread_mutex_unlock(&incoming_queue_mutex);

        log_debug("OPERATION: %d", r->operation);

        // We do not schedule OPEN and CLOSE requests so we can process them now
        if (r->operation == OPEN) {
            struct opened_handles *h = NULL;

            // Check if the file is already opened
            pthread_rwlock_wrlock(&handles_rwlock);
            HASH_FIND_STR(opened_files, r->file_name, h);

            if (h == NULL) {
                log_debug("OPEN FILE: %s", r->file_name);

                h = (struct opened_handles *) malloc(sizeof(struct opened_handles));

                if (h == NULL) {
                    MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
                }

                #ifdef PVFS
                // Generate an identifier for the PVFS file
                int fh = generate_pfs_identifier();
                
                // Open the file in PVFS
                int ret = PVFS_util_resolve(
                    r->file_name,
                    &(h->pvfs_file.fs_id),
                    h->pvfs_file.pvfs2_path,
                    PVFS_NAME_MAX
                );

                log_debug("----> (ret=%d) %s", ret, h->pvfs_file.pvfs2_path);

                strncpy(h->pvfs_file.user_path, r->file_name, PVFS_NAME_MAX);

                ret = generic_open(&h->pvfs_file, &credentials);

                if (ret < 0) {
                    log_debug("Could not open %s", r->file_name);

                    MPI_Abort(MPI_COMM_WORLD, ERROR_PVFS_OPEN);
                }
                #else
                int fh;

                if (simulation_direct_io == 1) {
                    fh = open(r->file_name, O_CREAT | O_RDWR | __O_DIRECT, 0666);
                } else {
                    fh = open(r->file_name, O_CREAT | O_RDWR, 0666);
                }
                
                if (fh < 0) {
                    log_debug("Could not open %s", r->file_name);

                    MPI_Abort(MPI_COMM_WORLD, ERROR_POSIX_OPEN);
                }
                #endif
                
                // Saves the file handle in the hash
                h->fh = fh;
                strcpy(h->path, r->file_name);
                h->references = 1;

                // Include in both hashes
                HASH_ADD(hh, opened_files, path, strlen(r->file_name), h);
                #ifdef PVFS
                HASH_ADD(hh_pvfs, opened_pvfs_files, fh, sizeof(int), h);
                #endif
            } else {
                struct opened_handles *tmp = (struct opened_handles *) malloc(sizeof(struct opened_handles));

                if (tmp == NULL) {
                    MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
                }
                
                // We need to increment the number of users of this handle
                h->references = h->references + 1;
                
                HASH_REPLACE(hh, opened_files, path, strlen(r->file_name), h, tmp);
                #ifdef PVFS
                HASH_REPLACE(hh_pvfs, opened_pvfs_files, fh, sizeof(int), h, tmp);
                #endif

                log_debug("FILE: %s", r->file_name);
            }

            // Release the lock that guaranteed an atomic update
            pthread_rwlock_unlock(&handles_rwlock);
            
            if (h == NULL) {
                MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
            }

            log_debug("FILE HANDLE: %d (references = %d)", h->fh, h->references);

            // Return the handle to be used in future operations
            MPI_Send(&h->fh, 1, MPI_INT, r->rank, TAG_HANDLE, MPI_COMM_WORLD);

            #ifdef STATISTICS
            // Update the statistics
            pthread_mutex_lock(&statistics_lock);
            statistics->open += 1;
            pthread_mutex_unlock(&statistics_lock);
            #endif

            // We can free the request as it has been processed
            safe_free(r, "server_listener::r");

            continue;
        }

        // Process the READ request
        if (r->operation == READ) {
            // Allow a thread in the pool to handle the incoming message
            thpool_add_work(thread_pool, (void*)handle_read, r);

            continue;
        } 
        
        // Process the WRITE request
        if (r->operation == WRITE) {
            // Allow a thread in the pool to handle the incoming message
            thpool_add_work(thread_pool, (void*)handle_write, r);

            continue;
        }

        // We do not schedule OPEN and CLOSE requests so we can process them now
        if (r->operation == CLOSE) {
            struct opened_handles *h = NULL;

            // Check if the file is already opened
            pthread_rwlock_wrlock(&handles_rwlock);
            HASH_FIND_STR(opened_files, r->file_name, h);

            if (h == NULL) {
                MPI_Abort(MPI_COMM_WORLD, ERROR_FAILED_TO_CLOSE);
            } else {
                // Update the number of users
                h->references = h->references - 1;

                // Check if we can actually close the file
                if (h->references == 0) {
                    log_debug("CLOSED: %s (%d)", h->path, h->fh);

                    #ifndef PVFS
                    // Close the file
                    close(h->fh);
                    #endif

                    // Remove the request from the hash
                    HASH_DELETE(hh, opened_files, h);
                    #ifdef PVFS
                    HASH_DELETE(hh_pvfs, opened_pvfs_files, h);
                    #endif
                    
                    safe_free(h, "server_listener::h");
                } else {
                    struct opened_handles *tmp = (struct opened_handles *) malloc(sizeof(struct opened_handles));

                    if (tmp == NULL) {
                        MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
                    }

                    HASH_REPLACE(hh, opened_files, path, strlen(r->file_name), h, tmp);
                    #ifdef PVFS
                    HASH_REPLACE(hh_pvfs, opened_pvfs_files, fh, sizeof(int), h, tmp);
                    #endif

                    log_debug("FILE HANDLE: %d (references = %d)", h->fh, h->references);
                }               
            }

            // Release the lock that guaranteed an atomic update
            pthread_rwlock_unlock(&handles_rwlock);

            // Return the handle to be used in future operations
            MPI_Send(&ack, 1, MPI_INT, r->rank, TAG_ACK, MPI_COMM_WORLD);

            #ifdef STATISTICS
            // Update the statistics
            pthread_mutex_lock(&statistics_lock);
            statistics->close += 1;
            pthread_mutex_unlock(&statistics_lock);
            #endif

            // We can free the request as it has been processed
            safe_free(r, "server_listener::r");

            continue;
        }

        safe_free(r, "server_listener::r");
        
        // Handle unknown request type
        MPI_Abort(MPI_COMM_WORLD, ERROR_UNKNOWN_REQUEST_TYPE);
    }
}

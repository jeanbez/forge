#include "server/handler.h"

/**
 * Handles the incoming requests, processing open and write calls, and scheduling read and write requests to AGIOS.
 */
void *server_handler(void *p) {
    int ack;
    char fh_str[255];

    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    timeout.tv_sec += TIMEOUT;
    
    MPI_Request request;
    MPI_Status status;

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
                int fh = open(r->file_name, O_CREAT | O_RDWR, 0666);
                
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
            // Include the request into the hash list
            pthread_rwlock_wrlock(&requests_rwlock);
            HASH_ADD_INT(requests, id, r);
            pthread_rwlock_unlock(&requests_rwlock);

            log_debug("add (handle: %d, operation: %d, offset: %ld, size: %ld, rank: %d, id: %ld)", r->file_handle, r->operation, r->offset, r->size, r->rank, r->id);

            sprintf(fh_str, "%015d", r->file_handle);

            #ifdef STATISTICS
            // Update the statistics
            pthread_mutex_lock(&statistics_lock);
            statistics->read += 1;
            statistics->read_size += r->size;
            pthread_mutex_unlock(&statistics_lock);
            #endif

            // Send the request to AGIOS
            if (agios_add_request(fh_str, r->operation, r->offset, r->size, (void *)r->id, &agios_client, 0)) {
                // Failed to sent to AGIOS, we should remove the request from the list
                log_debug("Failed to send the request to AGIOS");

                MPI_Abort(MPI_COMM_WORLD, ERROR_AGIOS_REQUEST);
            }

            continue;
        } 
        
        // Process the WRITE request
        if (r->operation == WRITE) {
            // Make sure the buffer can store the message
            r->buffer = calloc(r->size, sizeof(char));

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
            pthread_rwlock_wrlock(&requests_rwlock);
            HASH_ADD_INT(requests, id, r);
            pthread_rwlock_unlock(&requests_rwlock);
            
            log_debug("add (handle: %d, operation: %d, offset: %ld, size: %ld, id: %ld)", r->file_handle, r->operation, r->offset, r->size, r->id);

            sprintf(fh_str, "%015d", r->file_handle);

            #ifdef STATISTICS
            // Update the statistics
            pthread_mutex_lock(&statistics_lock);
            statistics->write += 1;
            statistics->write_size += r->size;
            pthread_mutex_unlock(&statistics_lock);
            #endif

            // Send the request to AGIOS
            if (agios_add_request(fh_str, r->operation, r->offset, r->size, (void *)r->id, &agios_client, 0)) {
                // Failed to sent to AGIOS, we should remove the request from the list
                log_debug("Failed to send the request to AGIOS");

                MPI_Abort(MPI_COMM_WORLD, ERROR_AGIOS_REQUEST);
            }

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

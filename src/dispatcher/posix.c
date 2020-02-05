#include "dispatcher/posix.h"

/**
 * Dispatch a POSIX request.
 * @param *aggregated A pointer to the aggregated request.
 */
void dispatch_operation(struct aggregated_request *aggregated) {
    if (aggregated->operation == READ) {
        dispatch_read(aggregated);
    } else {
        dispatch_write(aggregated);
    }
}

/**
 * Dispatch a POSIX read request.
 * @param *aggregated A pointer to the aggregated request.
 */
void dispatch_read(struct aggregated_request *aggregated) {
    // Iterate over the aggregated request, and issue them
    for (int i = 0; i < aggregated->count; i++) {
        int rc = pread(aggregated->requests[i].file_handle, aggregated->requests[i].buffer, aggregated->requests[i].size, aggregated->requests[i].offset);
    
        if (rc != aggregated->requests[i].size) {
            #ifdef EXPLAIN
            int err = errno;
            char message[3000];

            explain_message_errno_pread(message, sizeof(message), err, aggregated->requests[i].file_handle, aggregated->requests[i].buffer, aggregated->requests[i].size, aggregated->requests[i].offset);
            log_error("---> %s\n", message);
            #endif

            log_error("r->filehandle = %ld, r->size = %ld, r->offset = %ld", aggregated->requests[i].file_handle, aggregated->requests[i].size, aggregated->requests[i].offset);
            log_error("single read %d of expected %d", rc, aggregated->requests[i].size);
        }
    }

    thpool_add_work(thread_pool, (void*)callback_read, aggregated);
}

/**
 * Dispatch a POSIX write request.
 * @param *aggregated A pointer to the aggregated request.
 */
void dispatch_write(struct aggregated_request *aggregated) {
    // Iterate over the aggregated request, and issue them
    for (int i = 0; i < aggregated->count; i++) {
        int rc = pwrite(aggregated->requests[i].file_handle, aggregated->requests[i].buffer, aggregated->requests[i].size, aggregated->requests[i].offset);

        if (rc != aggregated->requests[i].size) {
            #ifdef EXPLAIN
            int err = errno;
            char message[3000];
            
            explain_message_errno_pwrite(message, sizeof(message), err, aggregated->requests[i].file_handle, aggregated->requests[i].buffer, aggregated->requests[i].size, aggregated->requests[i].offset);
            log_error("---> %s\n", message);
            #endif

            log_error("r->filehandle = %ld, r->size = %ld, r->offset = %ld", aggregated->requests[i].file_handle, aggregated->requests[i].size, aggregated->requests[i].offset);
            log_error("single write %d of expected %d", rc, aggregated->requests[i].size);
        }
    }

    thpool_add_work(thread_pool, (void*)callback_write, aggregated);
}

/**
 * Complete the aggregated read request by sending the data to the clients.
 * @param *aggregated A pointer to the aggregated request.
 */
void callback_read(struct aggregated_request *aggregated) {
    char fh_str[255];
    struct forwarding_request *current_r;


    // Iterate over the aggregated request, and reply to their clients
    for (int i = 0; i < aggregated->count; i++) {
        // Get and remove the request from the list    
        log_debug("aggregated[%d/%d] = %ld", i + 1, aggregated->count, aggregated->requests[i].id);

        pthread_rwlock_wrlock(&requests_rwlock);
        HASH_FIND_INT(requests, &aggregated->requests[i].id, current_r);

        if (current_r == NULL) {
            log_error("5. unable to find the request %lld", aggregated->requests[i].id);
        }
        HASH_DEL(requests, current_r);
        pthread_rwlock_unlock(&requests_rwlock);
        
        log_trace("operation = read, rank = %ld, request = %ld, offset = %ld (real = %ld), size = %ld", current_r->rank, current_r->id, current_r->offset, current_r->offset, current_r->size);
        
        MPI_Send(current_r->buffer, current_r->size, MPI_CHAR, current_r->rank, current_r->id, MPI_COMM_WORLD);

        // Release the AGIOS request
        sprintf(fh_str, "%015d", current_r->file_handle);

        agios_release_request(fh_str, current_r->operation, current_r->size, current_r->offset, 0, current_r->size); // 0 is a sub-request

        // Free the buffer and the request
        free(current_r->buffer);
        free(current_r);
    }

    free(aggregated);
}

/**
 * Complete the aggregated write request by sending the ACK to the clients.
 * @param *aggregated A pointer to the aggregated request.
 */
void callback_write(struct aggregated_request *aggregated) {
    int ack = 1;
    char fh_str[255];
    struct forwarding_request *current_r;

    // Iterate over the aggregated request, and reply to their clients
    for (int i = 0; i < aggregated->count; i++) {
        // Get and remove the request from the list    
        log_debug("aggregated[%d/%d] = %ld", i + 1, aggregated->count, aggregated->requests[i].id);

        pthread_rwlock_wrlock(&requests_rwlock);
        HASH_FIND_INT(requests, &aggregated->requests[i].id, current_r);

        if (current_r == NULL) {
            log_error("5. unable to find the request %lld", aggregated->requests[i].id);
        }
        HASH_DEL(requests, current_r);
        pthread_rwlock_unlock(&requests_rwlock);
        log_trace("operation = write, rank = %ld, request = %ld, offset = %ld (real = %ld), size = %ld", current_r->rank, current_r->id, current_r->offset, current_r->offset, current_r->size);

        MPI_Send(&ack, 1, MPI_INT, current_r->rank, TAG_ACK, MPI_COMM_WORLD); 

        // Release the AGIOS request
        sprintf(fh_str, "%015d", current_r->file_handle);

        agios_release_request(fh_str, current_r->operation, current_r->size, current_r->offset, 0, current_r->size); // 0 is a sub-request

        // Free the buffer and the request
        free(current_r->buffer);
        free(current_r);
    }

    free(aggregated);
}

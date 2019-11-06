#include "dispatcher/posix.h"

/**
 * Dispatch a POSIX read request.
 * @param *aggregated A pointer to the aggregated request.
 */
void dispatch_read(struct aggregated_request *aggregated) {
    int rc = pread(aggregated->r->file_handle, aggregated->buffer, aggregated->size, aggregated->r->offset);
    
    if (rc != aggregated->size) {
        int err = errno;
        char message[3000];

        explain_message_errno_pread(message, sizeof(message), err, aggregated->r->file_handle, aggregated->buffer, aggregated->size, aggregated->r->offset);
        log_error("---> %s\n", message);

        log_error("r->filehandle = %ld, r->size = %ld, r->offset = %ld", aggregated->r->file_handle, aggregated->size, aggregated->r->offset);
        log_error("single read %d of expected %d", rc, aggregated->size);
    }

    callback_read(aggregated);
}

/**
 * Dispatch a POSIX write request.
 * @param *aggregated A pointer to the aggregated request.
 */
void dispatch_write(struct aggregated_request *aggregated) {
    int rc = pwrite(aggregated->r->file_handle, aggregated->buffer, aggregated->size, aggregated->r->offset);

    if (rc != aggregated->size) {
        int err = errno;
        char message[3000];
        
        explain_message_errno_pwrite(message, sizeof(message), err, aggregated->r->file_handle, aggregated->buffer, aggregated->size, aggregated->r->offset);
        log_error("---> %s\n", message);

        log_error("r->filehandle = %ld, r->size = %ld, r->offset = %ld", aggregated->r->file_handle, aggregated->size, aggregated->r->offset);
        log_error("single write %d of expected %d", rc, aggregated->size);
    }

    callback_write(aggregated);
}

/**
 * Complete the aggregated read request by sending the data to the clients.
 * @param *aggregated A pointer to the aggregated request.
 */
void callback_read(struct aggregated_request *aggregated) {
    char fh_str[255];
    struct forwarding_request *current_r;

    #ifdef DEBUG
    char *tmp;
    #endif

    unsigned long int offset = 0;

    // Iterate over the aggregated request, and reply to their clients
    for (int i = 0; i < aggregated->count; i++) {
        // Get and remove the request from the list    
        log_debug("aggregated[%d/%d] = %ld", i + 1, aggregated->count, aggregated->ids[i]);

        pthread_mutex_lock(&requests_lock);
        HASH_FIND_INT(requests, &aggregated->ids[i], current_r);

        if (current_r == NULL) {
            log_error("5. unable to find the request %lld", aggregated->ids[i]);
        }
        HASH_DEL(requests, current_r);
        pthread_mutex_unlock(&requests_lock);
        
        #ifdef DEBUG
        log_trace("AGGREGATED={%s}", aggregated->buffer);
        
        tmp = calloc(current_r->size, sizeof(char));

        memcpy(tmp, &aggregated->buffer[offset], current_r->size * sizeof(char));

        log_debug("rank = %ld, request = %ld, buffer = %s, offset = %ld (real = %ld), size = %ld", current_r->rank, current_r->id, tmp, offset, current_r->offset, current_r->size);
        #endif

        MPI_Send(&aggregated->buffer[offset], current_r->size, MPI_CHAR, current_r->rank, TAG_BUFFER, MPI_COMM_WORLD);

        #ifdef DEBUG
        free(tmp);
        #endif

        // Release the AGIOS request
        sprintf(fh_str, "%015d", current_r->file_handle);

        agios_release_request(fh_str, current_r->operation, current_r->size, current_r->offset, 0, current_r->size); // 0 is a sub-request

        // Update to the next offset
        offset += current_r->size;

        // Free the request (not the buffer as it was not allocated)
        free(current_r);
    }

    free(aggregated->buffer);
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
        log_debug("aggregated[%d/%d] = %ld", i + 1, aggregated->count, aggregated->ids[i]);

        pthread_mutex_lock(&requests_lock);
        HASH_FIND_INT(requests, &aggregated->ids[i], current_r);

        if (current_r == NULL) {
            log_error("5. unable to find the request %lld", aggregated->ids[i]);
        }
        HASH_DEL(requests, current_r);
        pthread_mutex_unlock(&requests_lock);

        MPI_Send(&ack, 1, MPI_INT, current_r->rank, TAG_ACK, MPI_COMM_WORLD); 

        // Release the AGIOS request
        sprintf(fh_str, "%015d", current_r->file_handle);

        agios_release_request(fh_str, current_r->operation, current_r->size, current_r->offset, 0, current_r->size); // 0 is a sub-request

        // Free the request (not the buffer as it was not allocated)
        free(current_r->buffer);
        free(current_r);
    }

    free(aggregated->buffer);
    free(aggregated);
}

#include "handler/read.h"

int handle_read(struct forwarding_request *r) {
    int ret;
    char fh_str[255];
    
    // Make sure the buffer can store the message
    if (simulation_direct_io == 1) {
        ret = posix_memalign((void**) &r->buffer, getpagesize(), r->size);

        if (ret != 0) {
            log_error("unable to allocate aligned memory for O_DIRECT");

            MPI_Abort(MPI_COMM_WORLD, ERROR_MEMORY_ALLOCATION);
        }
    } else {
        r->buffer = calloc(r->size, sizeof(char));
    }

    // Send ACK to receive the buffer with the request ID
    MPI_Send(&r->id, 1, MPI_INT, r->rank, TAG_ACK, MPI_COMM_WORLD); 

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
    if (!agios_add_request(fh_str, r->operation, r->offset, r->size, r->id)) {
        // Failed to sent to AGIOS, we should remove the request from the list
        log_debug("Failed to send the request to AGIOS");

        MPI_Abort(MPI_COMM_WORLD, ERROR_AGIOS_REQUEST);
    }

    return 0;
}

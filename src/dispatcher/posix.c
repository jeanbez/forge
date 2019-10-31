#include "dispatcher/posix.h"

/**
 * Dispatch a POSIX read request.
 * @param *r A pointer to the forwarded request.
 * @param aggregated_size The size of the aggregated request.
 * @param aggregated_buffer A pointer to the buffer where the read data will be placed.
 * @return 0 if success, -1 if error.
 */
int dispatch_read(struct forwarding_request *r, int aggregated_size, char *aggregated_buffer) {
    int rc = pread(r->file_handle, aggregated_buffer, aggregated_size, r->offset);

    if (rc != aggregated_size) {
        int err = errno;
        char message[3000];

        explain_message_errno_pread(message, sizeof(message), err, r->file_handle, r->buffer, r->size, r->offset);
        log_error("---> %s\n", message);

        log_error("r->filehandle = %ld, r->size = %ld, r->offset = %ld", r->file_handle, r->size, r->offset);
        log_error("single read %d of expected %d", rc, aggregated_size);
        
        return -1;
    }

    return 0;
}

/**
 * Dispatch a POSIX write request.
 * @param *r A pointer to the forwarded request.
 * @param aggregated_size The size of the aggregated request.
 * @param aggregated_buffer A pointer to the buffer where the data to be written is.
 * @return 0 if success, -1 if error.
 */
int dispatch_write(struct forwarding_request *r, int aggregated_size, char *aggregated_buffer) {
    int rc = pwrite(r->file_handle, aggregated_buffer, aggregated_size, r->offset);

    if (rc != aggregated_size) {
        int err = errno;
        char message[3000];
        
        explain_message_errno_pwrite(message, sizeof(message), err, r->file_handle, r->buffer, r->size, r->offset);
        log_error("---> %s\n", message);

        log_error("r->filehandle = %ld, r->size = %ld, r->offset = %ld", r->file_handle, r->size, r->offset);
        log_error("single write %d of expected %d", rc, aggregated_size);
        
        return -1;
    }

    return 0;
}
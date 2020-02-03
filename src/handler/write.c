#include "handler/write.h"

int handle_write(struct forwarding_request *r) {
    int ret;
      char fh_str[255];

      MPI_Request request;
      MPI_Status status;

      // Make sure the buffer can store the message
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

      return 0;
}

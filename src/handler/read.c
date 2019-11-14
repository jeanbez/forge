#include "handler/read.h"

int handle_read(struct forwarding_request *r) {
      char fh_str[255];

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

      return 0;
}
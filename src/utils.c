#include "utils.h"
#include "log.h"

/**
 * Generate an unique identifier for the requests.
 * @return Unique ID for the request.
 */
unsigned long long int generate_identifier() {
    pthread_mutex_lock(&global_id_lock);
    global_id++;

    if (global_id > 9000000) {
        global_id = 1000000;
    }
    pthread_mutex_unlock(&global_id_lock);

    return global_id;
}

#ifdef PVFS
/**
 * Generate an unique identifier for PVFS/OrangeFS file handles.
 * @return Unique ID for the file handle.
 */
int generate_pfs_identifier() {
    pthread_mutex_lock(&pvfs_fh_id_lock);
    pvfs_fh_id++;

    if (pvfs_fh_id > INT_MAX - 10) {
        pvfs_fh_id = 100;
    }
    pthread_mutex_unlock(&pvfs_fh_id_lock);

    return pvfs_fh_id;
}
#endif

/**
 * Attempt to safely free a memory. Should be used for debug purposes.
 * @param **pointer_address The pointer address to free
 * @param *id A message to identify where the call was done.
 */
void safe_memory_free(void ** pointer_address, char *id) {
    if (pointer_address != NULL && *pointer_address != NULL) {
        free(*pointer_address);

        *pointer_address = NULL;
    } else {
        log_warn("double free or memory corruption was avoided: %s", id);
    }
}

void set_page_size() {
    page_size = getpagesize();
}

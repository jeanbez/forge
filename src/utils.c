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

void safe_memory_free(void ** pointer_address, char *id) {
    if (pointer_address != NULL && *pointer_address != NULL) {
        free(*pointer_address);

        *pointer_address = NULL;
    } else {
        log_warn("double free or memory corruption was avoided: %s", id);
    }
}

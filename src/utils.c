#include <stdio.h>
#include <stdlib.h>
#include "log.h"

void safe_memory_free(void ** pointer_address, char *id) {
    if (pointer_address != NULL && *pointer_address != NULL) {
        free(*pointer_address);

        *pointer_address = NULL;
    } else {
        log_warn("double free or memory corruption was avoided: %s", id);
    }
}
#define safe_free(pointer, id) safe_memory_free((void **) &(pointer), id)

void safe_memory_free(void ** pointer_address, char *id);
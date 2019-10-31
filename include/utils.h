#define safe_free(pointer, id) safe_memory_free((void **) &(pointer), id)

struct forwarding_statistics *statistics;
pthread_mutex_t statistics_lock;

void safe_memory_free(void ** pointer_address, char *id);
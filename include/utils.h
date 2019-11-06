#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define safe_free(pointer, id) safe_memory_free((void **) &(pointer), id)

struct forwarding_statistics *statistics;
pthread_mutex_t statistics_lock;

unsigned long long int global_id;
pthread_mutex_t global_id_lock;

unsigned long long int generate_identifier();

void safe_memory_free(void ** pointer_address, char *id);

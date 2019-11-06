#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <mpi.h>
#include <fcntl.h>
#include <math.h>
#include <unistd.h>
#include <limits.h>

#include <gsl/gsl_sort.h>
#include <gsl/gsl_statistics.h>

#include "jsmn.h"

unsigned long long int generate_identifier();

int get_forwarding_server();

void safe_memory_free(void ** pointer_address, char * id);

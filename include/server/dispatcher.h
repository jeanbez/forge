#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <mpi.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>

#include "forge.h"
#include "utils.h"
#include "pqueue.h"
#include "log.h"

typedef struct node_t {
	pqueue_pri_t priority;
	int value;
	size_t position;
} node_t;

void *server_dispatcher(void *p);
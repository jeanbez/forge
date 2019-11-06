#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <mpi.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>

#include "forge.h"
#include "utils.h"
#include "scheduler.h"
#include "log.h"

void *server_handler(void *p);
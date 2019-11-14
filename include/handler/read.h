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

int handle_read(struct forwarding_request *r);
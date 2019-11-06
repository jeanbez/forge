#include <mpi.h>
#include <agios.h>
#include <pthread.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "forge.h"
#include "log.h"

// PVFS variables needed for the direct integration
PVFS_offset file_req_offset;
PVFS_Request file_req, mem_req;
PVFS_sysresp_io resp_io;

void dispatch_read(struct aggregated_request *aggregated);
void dispatch_write(struct aggregated_request *aggregated);

void callback_read(struct aggregated_request *aggregated);
void callback_write(struct aggregated_request *aggregated);
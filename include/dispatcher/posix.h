#include <mpi.h>
#include <agios.h>
#include <pthread.h>
#include <errno.h>
#include <libexplain/pread.h>
#include <libexplain/pwrite.h>

#include "forge.h"
#include "log.h"

void dispatch_read(struct aggregated_request *aggregated);
void dispatch_write(struct aggregated_request *aggregated);

void callback_read(struct aggregated_request *aggregated);
void callback_write(struct aggregated_request *aggregated);

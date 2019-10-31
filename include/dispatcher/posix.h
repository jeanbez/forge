#include <errno.h>
#include <libexplain/pread.h>
#include <libexplain/pwrite.h>

#include "forge.h"
#include "log.h"

int dispatch_read(struct forwarding_request *r, int aggregated_size, char* aggregated_buffer);
int dispatch_write(struct forwarding_request *r, int aggregated_size, char *aggregated_buffer);
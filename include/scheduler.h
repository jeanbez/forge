#include <agios.h>
#include <mpi.h>

#include "forge.h"
#include "log.h"

// AGIOS client structure with pointers to callbacks
struct client agios_client;

void callback(unsigned long long int id);
void callback_aggregated(unsigned long long int *ids, int total);

void start_AGIOS(int simulation_forwarders);
void stop_AGIOS();
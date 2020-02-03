#include "dispatcher/orangefs.h"

int compare_offsets(const void* a, const void* b) {
    struct forwarding_request request_a = *(const struct forwarding_request*) a;
    struct forwarding_request request_b = *(const struct forwarding_request*) b;

    return (request_a.offset > request_b.offset); 
}
/**
 * Dispatch a PVFS request.
 * @param *aggregated A pointer to the aggregated request.
 */
void dispatch_operation(struct aggregated_request *aggregated) {
    int i, ret;
    const void* buffer = "";
    uint64_t offset = 0;

    // PVFS variables needed for the direct integration
    PVFS_Request mem_req, file_req;
    PVFS_sysresp_io resp_io;
    PVFS_credentials credentials;
    PVFS_size * displacements, * offsets;

    int32_t * sizes = (int32_t *) malloc(sizeof(int32_t) * aggregated->count);

    for (i = 0; i < aggregated->count; i++) {
        sizes[i] = aggregated->requests[i].size;
    }

    PVFS_util_gen_credentials(&credentials);

    if (aggregated->count == 1) {
    ret = PVFS_Request_contiguous(
            sizes[0],
            PVFS_BYTE,
        &mem_req
    );
    
    if (ret < 0) {
            log_error("PVFS_Request_contiguous failure");
    }

        buffer = (void *) aggregated->requests[0].buffer;
        offset = aggregated->requests[0].offset;

    ret = PVFS_Request_contiguous(
            sizes[0],
            PVFS_BYTE,
        &file_req
    );
    
        if (ret < 0) {
            log_error("PVFS_Request_contiguous failure");
        }
    } else {
        log_debug("sort the offsets");

        /*for (i = 0; i < aggregated->count; i++) {
            log_info("<---------------- %ld (id = %ld)", aggregated->requests[i].offset, aggregated->requests[i].id);
        }*/
        
        // List of requests to PVFS must be ordered by offset (ascending)6fcf
        qsort(&aggregated->requests, aggregated->count, sizeof(struct forwarding_request), compare_offsets);

        /*for (i = 0; i < aggregated->count; i++) {
            log_info("----------------> %ld (id = %ld)", aggregated->requests[i].offset, aggregated->requests[i].id);
    }
    
        log_debug("aggregated->count = %d", aggregated->count);

        for (i = 0; i < aggregated->count; i++) {
            log_info("operation = %d -> index[%d] size = %lld, offset = %lld", aggregated->operation, i, aggregated->requests[i].size, aggregated->requests[i].offset);
        }*/

        displacements = malloc(sizeof(PVFS_size) * aggregated->count);

        for (i = 0; i < aggregated->count; ++i) {
            displacements[i] = (intptr_t) aggregated->requests[i].buffer;
    }

        offsets = malloc(sizeof(PVFS_size) * aggregated->count);

        for (i = 0; i < aggregated->count; ++i) {
            offsets[i] = aggregated->requests[i].offset;
}

        ret = PVFS_Request_hindexed(
            aggregated->count,
            sizes,
            displacements,
            PVFS_BYTE,
            &mem_req
        );

        if (ret < 0) {
            log_error("PVFS_Request_indexed failure");
        }

        free(displacements);

        buffer = NULL;

        ret = PVFS_Request_hindexed(
            aggregated->count,
            sizes,
            offsets,
            PVFS_BYTE,
            &file_req
    );

    if (ret < 0) {
            log_error("READ PVFS_Request_indexed failure");
        }
    }

    struct opened_handles *h = NULL;

    pthread_rwlock_rdlock(&handles_rwlock);
    HASH_FIND(hh_pvfs, opened_pvfs_files, &aggregated->file_handle, sizeof(int), h);
        
    if (h == NULL) {
        log_error("unable to find the handle");
    }
    pthread_rwlock_unlock(&handles_rwlock);
    
    ret = PVFS_sys_io(
        h->pvfs_file.ref, 
        file_req, 
        offset,
        (void *) buffer, 
        mem_req, 
        &credentials, 
        &resp_io, 
        aggregated->operation,
        NULL
    );

    #ifdef DEBUG
    if (ret == 0) {
        log_debug("PVFS read %ld bytes\n", resp_io.total_completed);
    } else {
        PVFS_perror("PVFS_sys_read", ret);
    }
    #endif

    if (aggregated->operation == PVFS_IO_WRITE) {
    thpool_add_work(thread_pool, (void*)callback_write, aggregated);
    } else {
        thpool_add_work(thread_pool, (void*)callback_read, aggregated);
    }

    PVFS_Request_free(&mem_req);
    PVFS_Request_free(&file_req);

    free(sizes);
}

/**
 * Complete the aggregated read request by sending the data to the clients.
 * @param *aggregated A pointer to the aggregated request.
 */
void callback_read(struct aggregated_request *aggregated) {
    char fh_str[255];
    struct forwarding_request *current_r;


    // Iterate over the aggregated request, and reply to their clients
    for (int i = 0; i < aggregated->count; i++) {
        // Get and remove the request from the list    
        log_debug("aggregated[%d/%d] = %ld", i + 1, aggregated->count, aggregated->requests[i].id);

        pthread_rwlock_wrlock(&requests_rwlock);
        HASH_FIND_INT(requests, &aggregated->requests[i].id, current_r);

        if (current_r == NULL) {
            log_error("5. unable to find the request %lld", aggregated->requests[i].id);
        }
        HASH_DEL(requests, current_r);
        pthread_rwlock_unlock(&requests_rwlock);
        
        log_trace("operation = read, rank = %ld, request = %ld, buffer = %s, offset = %ld (real = %ld), size = %ld", current_r->rank, current_r->id, current_r->buffer, current_r->offset, current_r->offset, current_r->size);
        
        MPI_Send(current_r->buffer, current_r->size, MPI_CHAR, current_r->rank, current_r->id, MPI_COMM_WORLD);

        // Release the AGIOS request
        sprintf(fh_str, "%015d", current_r->file_handle);

        agios_release_request(fh_str, current_r->operation, current_r->size, current_r->offset, 0, current_r->size); // 0 is a sub-request

        // Free the buffer and the request
        free(current_r->buffer);
        free(current_r);
    }

    free(aggregated);
}

/**
 * Complete the aggregated write request by sending the ACK to the clients.
 * @param *aggregated A pointer to the aggregated request.
 */
void callback_write(struct aggregated_request *aggregated) {
    int ack = 1;
    char fh_str[255];
    struct forwarding_request *current_r;

    // Iterate over the aggregated request, and reply to their clients
    for (int i = 0; i < aggregated->count; i++) {
        // Get and remove the request from the list    
        log_debug("aggregated[%d/%d] = %ld", i + 1, aggregated->count, aggregated->requests[i].id);

        pthread_rwlock_wrlock(&requests_rwlock);
        HASH_FIND_INT(requests, &aggregated->requests[i].id, current_r);

        if (current_r == NULL) {
            log_error("5. unable to find the request %lld", aggregated->requests[i].id);
        }
        HASH_DEL(requests, current_r);
        pthread_rwlock_unlock(&requests_rwlock);
        log_trace("operation = write, rank = %ld, request = %ld, buffer = %s, offset = %ld (real = %ld), size = %ld", current_r->rank, current_r->id, current_r->buffer, current_r->offset, current_r->offset, current_r->size);

        MPI_Send(&ack, 1, MPI_INT, current_r->rank, TAG_ACK, MPI_COMM_WORLD); 

        // Release the AGIOS request
        sprintf(fh_str, "%015d", current_r->file_handle);

        agios_release_request(fh_str, current_r->operation, current_r->size, current_r->offset, 0, current_r->size); // 0 is a sub-request

        // Free the buffer and the request
        free(current_r->buffer);
        free(current_r);
    }

    free(aggregated);
}

/**
 * Opens a file in PVFS
 * @param *obj A pointer to a PVFS file object.
 * @param *credentials A pointer to the credtials object to access PVFS.
 */
int generic_open(pvfs2_file_object *obj, PVFS_credential *credentials) {

    PVFS_sysresp_lookup resp_lookup;
    PVFS_sysresp_getattr resp_getattr;
    PVFS_object_ref ref;

    int ret = -1;

    memset(&resp_lookup, 0, sizeof(PVFS_sysresp_lookup));

    ret = PVFS_sys_lookup(
        obj->fs_id, 
        (char *) obj->pvfs2_path,
        credentials, 
        &resp_lookup,
        PVFS2_LOOKUP_LINK_FOLLOW, NULL
    );
    
    if (ret < 0) {
        PVFS_perror("PVFS_sys_lookup", ret);
        return (-1);
    }

    ref.handle = resp_lookup.ref.handle;
    ref.fs_id = resp_lookup.ref.fs_id;

    memset(&resp_getattr, 0, sizeof(PVFS_sysresp_getattr));

    ret = PVFS_sys_getattr(
        ref,
        PVFS_ATTR_SYS_ALL,
        credentials,
        &resp_getattr,
        NULL
    );
    
    if (ret) {
        fprintf(stderr, "Failed to do pvfs2 getattr on %s\n", obj->pvfs2_path);
        return -1;
    }

    if (resp_getattr.attr.objtype != PVFS_TYPE_METAFILE) {
        fprintf(stderr, "Not a meta file!\n");
        return -1;
    }

    obj->perms = resp_getattr.attr.perms;
    memcpy(&obj->attr, &resp_getattr.attr, sizeof(PVFS_sys_attr));
    obj->attr.mask = PVFS_ATTR_SYS_ALL_SETABLE;
    obj->ref = ref;

    return 0;
}

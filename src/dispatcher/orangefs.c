#include "dispatcher/orangefs.h"

/**
 * Dispatch a PVFS read request.
 * @param *aggregated A pointer to the aggregated request.
 */
void dispatch_read(struct aggregated_request *aggregated) {
    int ret;

    // PVFS variables needed for the direct integration
    PVFS_offset file_req_offset;
    PVFS_Request file_req, mem_req;
    PVFS_sysresp_io resp_io;
    PVFS_credentials credentials;

    PVFS_util_gen_credentials(&credentials);

    // Buffer is contiguous in memory because of calloc
    ret = PVFS_Request_contiguous(
        aggregated->size,
        PVFS_CHAR,
        &mem_req
    );
    
    if (ret < 0) {
        log_error("READ PVFS_Request_contiguous in memory failed");
    }

    file_req_offset = aggregated->r->offset;

    ret = PVFS_Request_contiguous(
        aggregated->size,
        PVFS_CHAR,
        &file_req
    );
    
    struct opened_handles *h = NULL;

    pthread_rwlock_rdlock(&handles_rwlock);
    HASH_FIND(hh_pvfs, opened_pvfs_files, &aggregated->r->file_handle, sizeof(int), h);
        
    if (h == NULL) {
        log_error("unable to find the handle");
    }
    pthread_rwlock_unlock(&handles_rwlock);
    
    ret = PVFS_sys_read(
        h->pvfs_file.ref, 
        file_req, 
        file_req_offset,
        aggregated->buffer, 
        mem_req, 
        &credentials, 
        &resp_io, 
        NULL
    );

    #ifdef DEBUG
    if (ret == 0) {                
        log_debug("%ld\n", resp_io.total_completed);
    } else {
        PVFS_perror("PVFS_sys_read", ret);
    }
    #endif

    callback_read(aggregated);

    PVFS_Request_free(&mem_req);
    PVFS_Request_free(&file_req);
}

/**
 * Dispatch a PVFS write request.
 * @param *aggregated A pointer to the aggregated request.
 */
void dispatch_write(struct aggregated_request *aggregated) {
    int ret;

    // PVFS variables needed for the direct integration
    PVFS_Request mem_req;
    PVFS_sysresp_io resp_io;
    PVFS_credentials credentials;

    PVFS_util_gen_credentials(&credentials);

    // Buffer is contiguous in memory because of calloc
    ret = PVFS_Request_contiguous(
        aggregated->size,
        PVFS_CHAR,
        &mem_req
    );

    if (ret < 0) {
        log_error("WRITE PVFS_Request_contiguous in memory failed");
    }

    struct opened_handles *h = NULL;

    pthread_rwlock_rdlock(&handles_rwlock);
    HASH_FIND(hh_pvfs, opened_pvfs_files, &aggregated->r->file_handle, sizeof(int), h);
        
    if (h == NULL) {
        log_error("unable to find the handle");
    }
    pthread_rwlock_unlock(&handles_rwlock);
    
    log_trace("offset = {%ld} buffer = {%s}\n", aggregated->r->offset, aggregated->buffer);
    ret = PVFS_sys_write(
        h->pvfs_file.ref, 
        PVFS_BYTE, 
        aggregated->r->offset,
        aggregated->buffer,
        mem_req, 
        &credentials, 
        &resp_io, 
        NULL
    );

    #ifdef DEBUG
    if (ret == 0) {
        log_debug("%ld\n", resp_io.total_completed);
    } else {
        PVFS_perror("PVFS_sys_write", ret);
    }
    #endif

    callback_write(aggregated);

    PVFS_Request_free(&mem_req);
}

/**
 * Complete the aggregated read request by sending the data to the clients.
 * @param *aggregated A pointer to the aggregated request.
 */
void callback_read(struct aggregated_request *aggregated) {
    char fh_str[255];
    struct forwarding_request *current_r;

    #ifdef DEBUG
    char *tmp;
    #endif

    unsigned long int offset = 0;

    // Iterate over the aggregated request, and reply to their clients
    for (int i = 0; i < aggregated->count; i++) {
        // Get and remove the request from the list    
        log_debug("aggregated[%d/%d] = %ld", i + 1, aggregated->count, aggregated->ids[i]);

        pthread_rwlock_wrlock(&requests_rwlock);
        HASH_FIND_INT(requests, &aggregated->ids[i], current_r);

        if (current_r == NULL) {
            log_error("5. unable to find the request %lld", aggregated->ids[i]);
        }
        HASH_DEL(requests, current_r);
        pthread_rwlock_unlock(&requests_rwlock);
        
        #ifdef DEBUG
        log_trace("AGGREGATED={%s}", aggregated->buffer);
        
        tmp = calloc(current_r->size, sizeof(char));

        memcpy(tmp, &aggregated->buffer[offset], current_r->size * sizeof(char));

        log_debug("rank = %ld, request = %ld, buffer = %s, offset = %ld (real = %ld), size = %ld", current_r->rank, current_r->id, tmp, offset, current_r->offset, current_r->size);
        #endif

        MPI_Send(&aggregated->buffer[offset], current_r->size, MPI_CHAR, current_r->rank, TAG_BUFFER, MPI_COMM_WORLD);

        #ifdef DEBUG
        free(tmp);
        #endif

        // Release the AGIOS request
        sprintf(fh_str, "%015d", current_r->file_handle);

        agios_release_request(fh_str, current_r->operation, current_r->size, current_r->offset, 0, current_r->size); // 0 is a sub-request

        // Update to the next offset
        offset += current_r->size;

        // Free the request (not the buffer as it was not allocated)
        free(current_r);
    }

    free(aggregated->buffer);
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
        log_debug("aggregated[%d/%d] = %ld", i + 1, aggregated->count, aggregated->ids[i]);

        pthread_rwlock_wrlock(&requests_rwlock);
        HASH_FIND_INT(requests, &aggregated->ids[i], current_r);

        if (current_r == NULL) {
            log_error("5. unable to find the request %lld", aggregated->ids[i]);
        }
        HASH_DEL(requests, current_r);
        pthread_rwlock_unlock(&requests_rwlock);

        MPI_Send(&ack, 1, MPI_INT, current_r->rank, TAG_ACK, MPI_COMM_WORLD); 

        // Release the AGIOS request
        sprintf(fh_str, "%015d", current_r->file_handle);

        agios_release_request(fh_str, current_r->operation, current_r->size, current_r->offset, 0, current_r->size); // 0 is a sub-request

        // Free the request (not the buffer as it was not allocated)
        free(current_r->buffer);
        free(current_r);
    }

    free(aggregated->buffer);
    free(aggregated);
}

/**
 * Opens a file in PVFS
 * @param *obj A pointer to a PVFS file object.
 * @param *credentials A pointer to the credtials object to access PVFS.
 */
int generic_open(pvfs2_file_object *obj, PVFS_credential *credentials) {
    struct stat stat_buf;

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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "pvfs2.h"

#define PVFS 1

#define PINT_statfs_t struct statfs
#define PINT_statfs_fsid(_statfs) (_statfs)->f_fsid
#define PINT_statfs_fd_lookup(_fd, _statfs) fstatfs(_fd, (_statfs))

struct statfs {
    uint64_t f_type;
    uint64_t f_bsize;
    uint64_t f_blocks;
    uint64_t f_bfree;
    uint64_t f_bavail;
    uint64_t f_files;
    uint64_t f_ffree;
    fsid_t   f_fsid;
    uint64_t f_namelen;
};

enum open_type {
    OPEN_SRC,
    OPEN_DEST
};

enum object_type { 
    UNIX_FILE, 
    PVFS2_FILE 
};

typedef struct pvfs2_file_object_s {
    PVFS_fs_id fs_id;
    PVFS_object_ref ref;
    char pvfs2_path[PVFS_NAME_MAX];     
    char user_path[PVFS_NAME_MAX];
    PVFS_sys_attr attr;
    PVFS_permissions perms;
} pvfs2_file_object;

int generic_open(
    pvfs2_file_object *obj,
    PVFS_credentials *credentials
);
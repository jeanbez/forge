#include <stdio.h>
#include <stdlib.h>

#include "pvfs2.h"
#include "pvfs2-hint.h"

static PVFS_hint hints;

typedef struct pvfs2_file_object_s {
    PVFS_fs_id fs_id;
    PVFS_object_ref ref;
    char pvfs2_path[PVFS_NAME_MAX]; 
    char user_path[PVFS_NAME_MAX];
    PVFS_sys_attr attr;
    PVFS_permissions perms;
} pvfs2_file_object;

enum open_type {
    OPEN_SRC,
    OPEN_DEST
};

void make_attribs(PVFS_sys_attr *attr, PVFS_credentials *credentials,
                  int nr_datafiles, int mode);

int generic_open(pvfs2_file_object *obj, PVFS_credentials *credentials,
                        int nr_datafiles, PVFS_size strip_size, 
                        char *srcname, int open_type);
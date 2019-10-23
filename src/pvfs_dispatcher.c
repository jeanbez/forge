#include "pvfs_dispatcher.h"

void make_attribs(PVFS_sys_attr *attr, PVFS_credentials *credentials,
                  int nr_datafiles, int mode)
{
    attr->owner = credentials->uid; 
    attr->group = credentials->gid;
    attr->perms = PVFS_util_translate_mode(mode, 0);
    attr->mask = (PVFS_ATTR_SYS_ALL_SETABLE);
    attr->dfile_count = nr_datafiles;

    if (attr->dfile_count > 0)
    {
      attr->mask |= PVFS_ATTR_SYS_DFILE_COUNT;
    }
}    

/* generic_open:
 *  given a file_object, perform the apropriate open calls.  
 *  . the 'open_type' flag tells us if we can create the file if it does not
 *    exist: if it is the source, then no.  If it is the destination, then we
 *    will.  
 *  . If we are creating the file, nr_datafiles gives us the number of
 *    datafiles to use for the new file.
 *  . If 'srcname' is given, and the file is a directory, we will create a
 *    new file with the basename of srcname in the specified directory 
 */

int generic_open(pvfs2_file_object *obj, PVFS_credentials *credentials,
                        int nr_datafiles, PVFS_size strip_size, 
                        char *srcname, int open_type)
{
    struct stat stat_buf;
    PVFS_sysresp_lookup resp_lookup;
    PVFS_sysresp_getattr resp_getattr;
    PVFS_sysresp_create resp_create;
    PVFS_object_ref parent_ref;
    PVFS_sys_dist   *new_dist;
    int ret = -1;
    char *entry_name;           /* name of the pvfs2 file */
    char str_buf[PVFS_NAME_MAX];    /* basename of pvfs2 file */
 
    entry_name = str_buf;
 
    /* it's a PVFS2 file */
    if (strcmp(obj->pvfs2_path, "/") == 0)
    {
        /* special case: PVFS2 root file system, so stuff the end of
         * srcfile onto pvfs2_path */
        char *segp = NULL, *prev_segp = NULL;
        void *segstate = NULL;
        
        /* can only perform this special case if we know srcname */
        if (srcname == NULL)
        {
        printf("unable to guess filename in "
                        "toplevel PVFS2\n");
        return -1;
        }

        memset(&resp_lookup, 0, sizeof(PVFS_sysresp_lookup));
        ret = PVFS_sys_lookup(obj->fs_id, obj->pvfs2_path,
                                  credentials, &resp_lookup,
                                  PVFS2_LOOKUP_LINK_FOLLOW, hints);
        if (ret < 0)
        {
        PVFS_perror("PVFS_sys_lookup", ret);
        return (-1);
        }
        parent_ref.handle = resp_lookup.ref.handle;
        parent_ref.fs_id = resp_lookup.ref.fs_id;

        while (!PINT_string_next_segment(srcname, &segp, &segstate))
        {
        prev_segp = segp;
        }
        entry_name = prev_segp; /* see... points to basename of srcname */
    }
    else /* given either a pvfs2 directory or a pvfs2 file */
    {
        /* get the absolute path on the pvfs2 file system */
        
        /*parent_ref.fs_id = obj->pvfs2.fs_id; */

        if (PINT_remove_base_dir(obj->pvfs2_path,str_buf, 
                                     PVFS_NAME_MAX))
        {
        if(obj->pvfs2_path[0] != '/')
        {
            printf( "Error: poorly formatted path.\n");
        }
        printf( "Error: cannot retrieve entry name for "
            "creation on %s\n", obj->user_path);
        return(-1);
        }
        ret = PINT_lookup_parent(obj->pvfs2_path, 
                                     obj->fs_id, credentials,
                                     &parent_ref.handle);
        if (ret < 0)
        {
        PVFS_perror("PVFS_util_lookup_parent", ret);
        return (-1);
        }
        else /* parent lookup succeeded. if the pvfs2 path is just a
            directory, use basename of src for the new file */
        {
        int len = strlen(obj->pvfs2_path);
        if (obj->pvfs2_path[len - 1] == '/')
        {
            char *segp = NULL, *prev_segp = NULL;
            void *segstate = NULL;

            if (srcname == NULL)
            {
            printf( "unable to guess filename\n");
            return(-1);
            }
            while (!PINT_string_next_segment(srcname, 
                &segp, &segstate))
            {
            prev_segp = segp;
            }
            strncat(obj->pvfs2_path, prev_segp, PVFS_NAME_MAX);
            entry_name = prev_segp;
        }
        parent_ref.fs_id = obj->fs_id;
        }
    }

    memset(&resp_lookup, 0, sizeof(PVFS_sysresp_lookup));
    ret = PVFS_sys_ref_lookup(parent_ref.fs_id, entry_name,
                                  parent_ref, credentials, &resp_lookup,
                                  PVFS2_LOOKUP_LINK_FOLLOW, hints);

        if ((ret == 0) && (open_type == OPEN_SRC))
        {
            memset(&resp_getattr, 0, sizeof(PVFS_sysresp_getattr));
            ret = PVFS_sys_getattr(resp_lookup.ref, PVFS_ATTR_SYS_ALL_NOHINT,
                                   credentials, &resp_getattr, hints);
            if (ret)
            {
                printf( "Failed to do pvfs2 getattr on %s\n",
                        entry_name);
                return -1;
            }

            if (resp_getattr.attr.objtype == PVFS_TYPE_SYMLINK)
            {
                free(resp_getattr.attr.link_target);
                resp_getattr.attr.link_target = NULL;
            }
            obj->perms = resp_getattr.attr.perms;
            memcpy(&obj->attr, &resp_getattr.attr,
                   sizeof(PVFS_sys_attr));
            obj->attr.mask = PVFS_ATTR_SYS_ALL_SETABLE;
        }

    /* at this point, we have looked up the file in the parent directory.
     * . If we found something, and we are the SRC, then we're done. 
     * . We will maintain the semantic of pvfs2-import and refuse to
     *   overwrite existing PVFS2 files, so if we found something, and we
     *   are the DEST, then that's an error.  
     * . Otherwise, we found nothing and we will create the destination. 
     */
    if (open_type == OPEN_SRC)
    {
        if (ret == 0)
        {
        obj->ref = resp_lookup.ref;
        return 0;
        }
        else
        {
        PVFS_perror("PVFS_sys_ref_lookup", ret);
        return (ret);
        }
    }
    if (open_type == OPEN_DEST)
    {
        if (ret == 0)
        {
                obj->ref = resp_lookup.ref;
        return 0;
        } 
        else 
        {
                memset(&stat_buf, 0, sizeof(struct stat));

                /* preserve permissions doing a unix => pvfs2 copy */
                stat(srcname, &stat_buf);
                make_attribs(&(obj->attr), credentials, nr_datafiles,
                             (int)stat_buf.st_mode);
                if (strip_size > 0) {
                    new_dist = PVFS_sys_dist_lookup("simple_stripe");
                    ret = PVFS_sys_dist_setparam(new_dist, "strip_size", &strip_size);
                    if (ret < 0)
                    {
                       PVFS_perror("PVFS_sys_dist_setparam", ret); 
               return -1; 
                    }
                }
                else {
                    new_dist=NULL;
                }
            
        ret = PVFS_sys_create(entry_name, parent_ref, 
                                      obj->attr, credentials,
                                      new_dist, &resp_create, NULL, hints);
        if (ret < 0)
        {
            PVFS_perror("PVFS_sys_create", ret); 
            return -1; 
        }
        obj->ref = resp_create.ref;
        }
    }
    return 0;
}
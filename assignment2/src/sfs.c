/*
  Simple File System

  This code is derived from function prototypes found /usr/include/fuse/fuse.h
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  His code is licensed under the LGPLv2.

*/

#include "params.h"
#include "block.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#include "log.h"


///////////////////////////////////////////////////////////
//
// Prototypes for all these functions, and the C-style comments,
// come indirectly from /usr/include/fuse.h
//

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
struct super_block* sb;
struct index_table *it;
static const char *hello_path="/home/anagha/testfolder";
void *sfs_init(struct fuse_conn_info *conn)
{
    
    log_msg("Testing- log started- sfs_init()\n");
    log_msg("\nsfs_init()\n");
    
    log_conn(conn);
    log_fuse_context(fuse_get_context());
    
    disk_open(SFS_DATA->diskfile);
    char *readbuf;
    char *writebuf;
    int status=block_read(0,&readbuf);
    
    //writebuf=&sb;
    
    if(status==0){
        /**
         * Disk file is accessed for the first time. We need to create a super block and initialize 
         * its members. Also, need to write this information to disk for persistence.
         */
        
        sb = (struct super_block*)malloc(sizeof(struct super_block));

        sb->size=131072;
        sb->nblocks=256;
        sb->ninode=0;

        sb->inode_begin=2;
        sb->next_free_inode=2;

        sb->data_block_begin=258;
        sb->next_free_block=257;
        
    
        writebuf=sb;
        int write_status=block_write(0,&writebuf);
        if(write_status>0){
            log_msg("Super block information was successfully written to disk\n");
        }
        else{
            log_msg("Error writing to disk\n");
        }
        /**
         *Also, create a struct for index table and write to disk
         */
        it = (struct index_table*)malloc(256*sizeof(struct index_table));
        writebuf=it;
        write_status=block_write(1,&writebuf);
        if(write_status>0){
            log_msg("Index table information was successfully written to disk\n");
        }
        else{
            log_msg("Error writing to disk\n");
        }
        
    }
    else if(status>0){
            /**
            * This means disk was accessed before. We need to load super block information
            * and  index table information from the disk. 
            */

            struct super_block* testbuf;
            struct it* testbuf2;
            
            int status=block_read(0,&testbuf);
            sb=testbuf;
            if(status>0){
                log_msg("Super block was read successfully\n");
            }
            else{
                log_msg("Error reading from disk\n");
            }

            status=block_read(1,&testbuf2);
            it=testbuf2;

            if(status>0){
                log_msg("Index table was read successfully\n");
            }
            else{
                log_msg("Error reading from disk\n");
            }

   }
    else{
        log_msg("Error in sfs_init, Can't read block\n");
    }
    disk_close();
    log_msg("Testing- exiting sfs_init()\n");
    return SFS_DATA;
}

/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */


void sfs_destroy(void *userdata)
{
    log_msg("Testing- in sfs_destroy()\n");
    log_msg("\nsfs_destroy(userdata=0x%08x)\n", userdata);
    log_msg("Testing- exiting sfs_destroy()\n");
}

/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int sfs_getattr(const char *path, struct stat *statbuf)
{
    int retstat = 0;
    
    log_msg("Testing- in sfs_getattr()\n");
    log_msg("\nsfs_getattr(path=\"%s\", statbuf=0x%08x)\n",
	  path, statbuf);   
    /**
     * To get file statistics, traverse the index table to get inode number corresponding to the given path.
     * Once, we have the inode number, get inode statistics from the corresponding block.
     */
    int i,val;
    for(i=0;i<sb->ninode;i++){
      if(strcmp(it[i].path,path)==0){  /*We found a match!*/
            val=it[i].inode;
            break;
      }
    }

    int status=block_read(val,&statbuf);
    if(status>0){
        log_msg("Data retrieved successfully %d\n",status);        
    }
    else{
        log_msg("Error reading from block\n");
        retstat=-1;
    }
    
    
    log_msg("Testing- exiting sfs_getattr()\n");
    return retstat;
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
int sfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    int retstat = 0;
    char *writebuf;
    log_msg("Testing- in sfs_create()\n");
    log_msg("\nsfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n",
        path, mode, fi);

    /**
     * Allocate memory for a new inode structure. Populate structure members according to the superblock values 
     * and write this information to disk.
     */

    struct inode* new_inode=(struct inode *)malloc(sizeof(struct inode));
    new_inode->st_ino=sb->next_free_inode;
    new_inode->st_mode=mode;
    new_inode->st_nlink=1;
    new_inode->st_uid=getpwnam();
    new_inode->st_gid=getgrgid();
    new_inode->st_size=0;
    new_inode->st_blksize=512;
    new_inode->st_blocks=1;
    new_inode->data_block=sb->next_free_block;
    new_inode->i_type='F';

    new_inode->st_atime=time(0);
    new_inode->st_mtime=time(0);
    new_inode->st_ctime=time(0);

    it[sb->next_free_inode].inode=new_inode->st_ino;
    it[sb->next_free_inode].path=path;

    sb->next_free_inode=sb->next_free_inode+1;
    sb->next_free_block=sb->next_free_block+1;

    writebuf=sb;
    int write_status=block_write(0,&writebuf);
    if(write_status>0){
        log_msg("Super block information was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }
    
    
    writebuf=it;
    write_status=block_write(1,&writebuf);
    if(write_status>0){
        log_msg("Index table information was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }

    writebuf=new_inode;
    write_status=block_write(new_inode->st_ino,&writebuf);
    if(write_status>0){
        log_msg("Inode was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }


    log_msg("Testing- exiting sfs_create()\n");
    return retstat;
}

/** Remove a file */
int sfs_unlink(const char *path)
{
    int retstat = 0;
    log_msg("Testing- in sfs_unlink()\n");
    log_msg("sfs_unlink(path=\"%s\")\n", path);
    
    /**
     * Delete the file's entry from the index table and change super block members accordingly.
     */
    
    int i, val,j;
    for(i=0;i<sb->ninode;i++){
      if(strcmp(it[i].path,path)==0){  /*We found the entry to be deleted*/
            val=it[i].inode;
            it[i].path=NULL;
            it[i].inode=-1;
            break;
            

      }
    }

    //Also check if that inode is present in the child-nodes list, if yes delete it 

    for(i=0;i<sb->ninode-1;i++){
      for(j=0;j<(sizeof(it[i].child_nodes)/sizeof(int));j++){
        if(it[i].child_nodes[j]==val){
            it[i].child_nodes[j]=NULL;
        }
      }
    }

    sb->next_free_inode=sb->next_free_inode-1;
    sb->next_free_block=sb->next_free_block-1;
    
    char* writebuf;
    writebuf=sb;
    int write_status=block_write(0,&writebuf);
    if(write_status>0){
        log_msg("Super block information was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }
    
    
    writebuf=it;
    write_status=block_write(1,&writebuf);
    if(write_status>0){
        log_msg("Index table information was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }

    log_msg("Testing- exiting sfs_unlink()\n");
    return retstat;
}

/** File open operation
 *
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  Optionally open may also
 * return an arbitrary filehandle in the fuse_file_info structure,
 * which will be passed to all file operations.
 *
 * Changed in version 2.2
 */
int sfs_open(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    int block_num,i;
    int file_descriptor;

    log_msg("Testing- in sfs_open()\n");
    log_msg("\nsfs_open(path\"%s\", fi=0x%08x)\n",
	    path, fi);
    /**
     * We need to go through index table to get inode corresponding to the path.
     * Then we fetch the inode and data block for that inode.
     * This data block is returned as the file descriptor. It is also added to the fi->fh of
     * fuse_file_info struct.
     */
    for(i=0;i<sb->ninode;i++){
      if(strcmp(it[i].path,path)==0){  /*We found a match!*/
            block_num=it[i].inode;
            struct inode* readbuf;
            
            int status=block_read(block_num,&readbuf);
            if(status>0){
                file_descriptor=readbuf->data_block;
                fi->fh=file_descriptor;
            }
            else{
                log_msg("Error reading block\n");
                retstat=-1;
            }
            break;
      }
    }


    log_msg("Testing- exiting sfs_open()\n");
    return retstat;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
int sfs_release(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("Testing- in sfs_release()\n");
    log_msg("\nsfs_release(path=\"%s\", fi=0x%08x)\n",
	  path, fi);

    /**
     * Clear the fi->fh field of fuse_file_info so that
     * read and write operations will not be performed on the file
     * and the file is released.
     */
    if(fi->fh>0){
        fi->fh=-1;

    }
    else{
        retstat=-1;
    }
    
    log_msg("Testing- exiting sfs_release()\n");
    return retstat;
}

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
int sfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("\nsfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);

    int block_num;
    int i;
    int file_descriptor;

    /**
     * Iterate through the index table to get inode corresponding to path.
     * Get the data block from inode and read contents of the buffer to
     * the position specified by offset. 
     */

    for(i=0;i<sb->ninode;i++){
      if(strcmp(it[i].path,path)==0){  
            block_num=it[i].inode;
            struct inode* readbuf;
            
            int status=block_read(block_num,&readbuf);
            if(status>0){
                file_descriptor=readbuf->data_block;
                retstat=pread(file_descriptor,&buf,size,offset);
            }
            else{
                log_msg("Error reading block\n");
            }
            break;
      }
    }

    return retstat;
}

/** Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 *
 * Changed in version 2.2
 */
int sfs_write(const char *path, const char *buf, size_t size, off_t offset,
	     struct fuse_file_info *fi)
{
    int retstat = 0;
    int block_num;
    int i;
    int file_descriptor;

    log_msg("\nsfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
    /**
     * Iterate through the index table to get inode corresponding to path.
     * Get the data block from inode and write contents of the buffer to
     * the position specified by offset. 
     */
    for(i=0;i<sb->ninode;i++){
      if(strcmp(it[i].path,path)==0){  /*We found a match!*/
            block_num=it[i].inode;
            struct inode* readbuf;
            
            int status=block_read(block_num,&readbuf);
            if(status>0){
                file_descriptor=readbuf->data_block;
                retstat=pwrite(file_descriptor,&buf,size,offset);
            }
            else{
                log_msg("Error reading block\n");
            }
            break;
      }
    }
    
    return retstat;
}


/** Create a directory */
int sfs_mkdir(const char *path, mode_t mode)
{
    int retstat = 0;
    char* writebuf;
    log_msg("\nsfs_mkdir(path=\"%s\", mode=0%3o)\n",
	    path, mode);


    /**
     * Allocate memory for a new inode structure. Populate structure members according to the superblock values 
     * and write this information to disk.
     */

    struct inode* new_inode=(struct inode *)malloc(sizeof(struct inode));
    new_inode->st_ino=sb->next_free_inode;
    new_inode->st_mode=mode;
    new_inode->st_nlink=1;
    new_inode->st_uid=getpwnam();
    new_inode->st_gid=getgrgid();
    new_inode->st_size=0;
    new_inode->st_blksize=512;
    new_inode->st_blocks=1;
    new_inode->data_block=sb->next_free_block;
    new_inode->i_type='D';

    new_inode->st_atime=time(0);
    new_inode->st_mtime=time(0);
    new_inode->st_ctime=time(0);

    it[sb->next_free_inode].inode=new_inode->st_ino;
    it[sb->next_free_inode].path=path;

    sb->next_free_inode=sb->next_free_inode+1;
    sb->next_free_block=sb->next_free_block+1;

    writebuf=sb;
    int write_status=block_write(0,&writebuf);
    if(write_status>0){
        log_msg("Super block information was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }
    
    
    writebuf=it;
    write_status=block_write(1,&writebuf);
    if(write_status>0){
        log_msg("Index table information was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }

    writebuf=new_inode;
    write_status=block_write(new_inode->st_ino,&writebuf);
    if(write_status>0){
        log_msg("Inode was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }

   
    
    return retstat;
}


/** Remove a directory */
int sfs_rmdir(const char *path)
{
    int retstat = 0;
    int i;
    log_msg("sfs_rmdir(path=\"%s\")\n",path);

    /**
     * Find the inode for corresponding directory path. Check if it's a Directory.
     * Check if the Directory is empty. Then delete Directory entry from Index-table.
     * Update Super-block accordingly.
     **/

    int val,j;
    for(i=0;i<sb->ninode;i++){
      if(strcmp(it[i].path,path)==0){  /*We found the entry to be deleted*/
            val=it[i].inode;

            struct inode* readbuf;
            
            int status=block_read(val,&readbuf);
            if(status>0 && readbuf->i_type=='D'){
                if(it[i].child_nodes[0] == -1){    // Checking if the directory is empty
                    it[i].path=NULL;
                    it[i].inode=-1;
                }
                else{
                    log_msg("Error : Directory not empty\n");
                }
            }
            else{
                log_msg("Error reading block\n");
            }
            break;
        }
    }

    //Also check if that inode is present in the child-nodes list, if yes delete it 

    for(i=0;i<sb->ninode-1;i++){
      for(j=0;j<(sizeof(it[i].child_nodes)/sizeof(int));j++){
        if(it[i].child_nodes[j]==val){
            it[i].child_nodes[j]=NULL;
        }
      }
    }

    sb->next_free_inode=sb->next_free_inode-1;
    sb->next_free_block=sb->next_free_block-1;
    
    char* writebuf;
    writebuf=sb;
    int write_status=block_write(0,&writebuf);
    if(write_status>0){
        log_msg("Super block information was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }
    
    
    writebuf=it;
    write_status=block_write(1,&writebuf);
    if(write_status>0){
        log_msg("Index table information was successfully written to disk\n");
    }
    else{
        log_msg("Error writing to disk\n");
        retstat=-1;
    }

    
    
    return retstat;
}


/** Open directory
 *
 * This method should check if the open operation is permitted for
 * this  directory
 *
 * Introduced in version 2.3
 */
int sfs_opendir(const char *path, struct fuse_file_info *fi)
{
    int retstat=0;
    log_msg("Testing - In sfs_opendir\n");

    log_msg("Testing- Exiting opendir\n");
    return retstat;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 * Introduced in version 2.3
 */
int sfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
	       struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("Testing- in sfs_readdir()\n");
    log_msg("\nsfs_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n",
        path, buf, filler, offset, fi);
    /**
     * To read a directory, go through the index table to find entry for that directory's inode.
     * When the match is found, iterate through it's child nodes and their paths to buffer
     * using filler function.
     */
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    
    int i,j;
    for(i=0;i<sb->ninode;i++){
      if(strcmp(it[i].path,path)==0){ 
            for(j=0;j<(sizeof(it[i].child_nodes)/sizeof(int));j++){
                if(it[j].inode==it[i].child_nodes[j]){
                    log_msg("Adding path %s\n",it[j].path);
                    filler(buf, it[j].path, NULL, 0);
                }
                
            }
            break;
      }
      else
        retstat=-1;
    }

    /*DIR *dp;
    struct dirent *de;
    dp = opendir(path);

    while((de=readdir(dp))!=NULL){
     log_msg("Adding file %s\n", de->d_name);
     filler(buf, de->d_name, NULL, 0);

    }*/

    return retstat;

}

/** Release directory
 *
 * Introduced in version 2.3
 */
int sfs_releasedir(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;

    
    return retstat;
}

struct fuse_operations sfs_oper = {
  .init = sfs_init,
  .destroy = sfs_destroy,

  .getattr = sfs_getattr,
  .create = sfs_create,
  .unlink = sfs_unlink,
  .open = sfs_open,
  .release = sfs_release,
  .read = sfs_read,
  .write = sfs_write,

  .rmdir = sfs_rmdir,
  .mkdir = sfs_mkdir,

  .opendir = sfs_opendir,
  .readdir = sfs_readdir,
  .releasedir = sfs_releasedir
};

void sfs_usage()
{
    fprintf(stderr, "usage:  sfs [FUSE and mount options] diskFile mountPoint\n");
    abort();
}

int main(int argc, char *argv[])
{
    int fuse_stat;
    struct sfs_state *sfs_data;
    
    // sanity checking on the command line
    if ((argc < 3) || (argv[argc-2][0] == '-') || (argv[argc-1][0] == '-'))
	sfs_usage();

    sfs_data = malloc(sizeof(struct sfs_state));
    if (sfs_data == NULL) {
	perror("main calloc");
	abort();
    }

    // Pull the diskfile and save it in internal data
    sfs_data->diskfile = argv[argc-2];
    //char* rootdir = realpath(argv[argc-1],NULL);
    argv[argc-2] = argv[argc-1];
    argv[argc-1] = NULL;
    argc--;
    
    sfs_data->logfile = log_open();
    
    // turn over control to fuse
    
    fprintf(stderr, "about to call fuse_main, %s \n", sfs_data->diskfile);
    fuse_stat = fuse_main(argc, argv, &sfs_oper, sfs_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);
   
    return fuse_stat;
}

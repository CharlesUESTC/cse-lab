// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 

    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    // return ! isfile(inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 

    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    const char *cdir, *pcur;
    const chfs_dirent *pdir;
    chfs_dirent *pent;
    unsigned dsize;
    unsigned nsz = strlen(name);
    std::string sdir;
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("create %s\n", name);

    if (!isdir(parent)) {
        r = NOENT;
        goto release;
    }

    if (nsz == 0 || nsz > CHFS_NAME_LEN) {
        printf("error creating: name is null or too long\n");
        r = NOENT;
        goto release;
    }

    // lookup name in parent dir
    ino_out = 0;
    if (ec->get(parent, sdir) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    dsize = sdir.size();
    cdir = sdir.c_str();
    pcur = cdir;

    while (pcur < cdir + dsize) {
        pdir = (const chfs_dirent *) pcur;
        if (namecmp(name, pdir->name, pdir->name_len) == 0) {
            r = EXIST;
            goto release;
        }
        pcur += pdir->rec_len;
    }

    // alloc inode
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    // update parent dir
    char new_ent[CHFS_DIRENT_SIZE];
    pent = (chfs_dirent *) new_ent;
    pent->inum = ino_out;
    pent->name_len = nsz;
    pent->rec_len = 8 + pent->name_len;
    if (pent->rec_len % 4 != 0)
        pent->rec_len += (4 - pent->rec_len % 4);
    pent->file_type = extent_protocol::T_FILE;
    memcpy(pent->name, name, nsz);

    sdir.append(new_ent, pent->rec_len);
    if (ec->put(parent, sdir) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    const char *cdir, *pcur;
    const chfs_dirent *pdir;
    unsigned dsize;
    std::string sdir;
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    printf("lookup %s\n", name);

    if (!isdir(parent)) {
        r = NOENT;
        goto release;
    }

    found = false;
    ino_out = 0;
    if (ec->get(parent, sdir) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    dsize = sdir.size();
    cdir = sdir.c_str();
    pcur = cdir;

    while (pcur < cdir + dsize) {
        pdir = (const chfs_dirent *) pcur;
        if (namecmp(name, pdir->name, pdir->name_len) == 0) {
            ino_out = pdir->inum;
            found = true;
            break;
        }
        pcur += pdir->rec_len;
    }

release:
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    const char *cdir, *pcur;
    const chfs_dirent *pdir;
    unsigned dsize;
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    printf("readdir %016llx\n", dir);

    std::string sdir;
    if (ec->get(dir, sdir) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    dsize = sdir.size();
    cdir = sdir.c_str();
    pcur = cdir;

    while (pcur < cdir + dsize) {
        pdir = (const chfs_dirent *) pcur;
        std::string ename(pdir->name, pdir->name_len);
        list.emplace_back(ename, pdir->inum);
        pcur += pdir->rec_len;
    }

release:
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    return r;
}

int namecmp(const char *name, const char *ent, uint8_t len)
{
    const char *s = ent;
    unsigned char c1, c2;

    for (; ;) {
        c1 = *name++;
        c2 = *s++;
        if (c1 != c2)
            return c1 < c2 ? -1 : 1;
        if (s == ent + len)
            return *name ? 1 : 0;
    }
}

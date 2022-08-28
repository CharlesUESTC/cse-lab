#include "inode_manager.h"
#include <cstring>
#include <ctime>
#include <utility>

// Block containing free inode bitmap
#define FIBBLOCK (BLOCK_NUM/BPB + 2)

// First block containg data
#define FDBLOCK (BLOCK_NUM/BPB + INODE_NUM/IPB + 3)

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  // Check from the bit in bitmap corresponding to the first data block
  // find a "0" bit, update the bit and mark in using_blocks
  blockid_t bnum;
  char buf[BLOCK_SIZE];
  bool empty = false;
  int i, j, k;
  int len = BLOCK_NUM / BPB;
  int offset = FDBLOCK % BPB;
  int start = BBLOCK(FDBLOCK);
  unsigned char mask;

  i = start;
  d->read_block(i, buf);
  j = offset / 8;
  mask = 0x80;
  k = offset % 8;
  mask = mask >> k;

  for (; k < 8; ++k) {
    if ((buf[j] & mask) == 0) {
        buf[j] = buf[j] | mask;
        empty = true;
        break;
    }
    mask = mask >> 1;
  }

  if (!empty) {
    for (++j; j < BLOCK_SIZE; ++j) {
      mask = 0x80;

      for (k = 0; k < 8; ++k) {
        if ((buf[j] & mask) == 0) {
          buf[j] = buf[j] | mask;
          empty = true;
          break;
        }
        mask = mask >> 1;
      }

      if (empty)
        break;
    }
  }
  d->write_block(i, buf);

  if (!empty) {
    for (++i; i < len; ++i) {
      d->read_block(i, buf);

      for (j = 0; j < BLOCK_SIZE; ++j) {
        mask = 0x80;

        for (k = 0; k < 8; ++k) {
          if ((buf[j] & mask) == 0) {
            buf[j] = buf[j] | mask;
            empty = true;
            break;
          }
          mask = mask >> 1;
        }

        if (empty)
          break;
      }

      d->write_block(i, buf);
      if (empty)
        break;
    }
  }

  if (empty) {
    bnum = (i - start) * BPB + j * 8 + k;
    using_blocks.insert(std::pair<uint32_t, int>(bnum, 0));
  }
  else {
    printf("No extra block to allocate.\n");
    exit(1);
  }
  
  return bnum;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  uint32_t bblock_id;
  int offset, index;
  char buf[BLOCK_SIZE];
  unsigned char mask;

  if (id < FDBLOCK || id >= BLOCK_NUM) {
    printf("\tbm: block id out of range\n");
    return;
  }

  using_blocks.erase(id);

  bblock_id = BBLOCK(id);
  offset = id % BPB;
  d->read_block(bblock_id, buf);
  mask = 0x80;
  mask = ~(mask >> offset % 8);
  index = offset / 8;
  buf[index] = buf[index] & mask;
  d->write_block(bblock_id, buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();
  char buf[BLOCK_SIZE];

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
  
  *((superblock_t *) buf) = sb;

  d->write_block(1, buf);
}

block_manager::~block_manager()
{
    delete d;
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  char buf[BLOCK_SIZE];
  bm = new block_manager();
  bm->read_block(FIBBLOCK, buf);
  buf[0] |= 0x80;
  bm->write_block(FIBBLOCK, buf);
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

inode_manager::~inode_manager()
{
    delete bm;
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  inode_t ino;
  uint32_t inum;
  char buf[BLOCK_SIZE];
  int len = INODE_NUM / 8;
  bool empty = false;
  int i, j;

  bm->read_block(FIBBLOCK, buf);

  for (i = 0; i < len; ++i) {
    unsigned char mask = 0x80;

    for (j = 0; j < 8; ++j) {
      if ((buf[i] & mask) == 0) {
        buf[i] = buf[i] | mask;
        empty = true;
        break;
      }
      mask = mask >> 1;
    }

    if (empty)
      break;
  }

  if (empty)
    inum = i * 8 + j;
  else {
    printf("No extra inode to allocate.\n");
    exit(1);
  }

  ino.type = type;
  ino.size = 0;
  ino.atime = (unsigned int) time(NULL);
  ino.mtime = (unsigned int) time(NULL);
  ino.ctime = (unsigned int) time(NULL);

  bm->write_block(FIBBLOCK, buf);
  put_inode(inum, &ino);

  return inum;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t *ino;
  char buf[BLOCK_SIZE];
  int index, offset;
  unsigned char mask;

  index = inum / 8;
  offset = inum % 8;
  bm->read_block(FIBBLOCK, buf);
  mask = 0x80;
  mask = mask >> offset;

  if ((buf[index] & mask) != 0) {
    mask = ~mask;
    buf[index] &= mask;
    bm->write_block(FIBBLOCK, buf);
  }

  ino = get_inode(inum);
  if (ino != NULL) {
    ino->type = 0;
    put_inode(inum, ino);
    free(ino);
  }
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];
  //unsigned char mask;
  //int index, offset;

  printf("\tim: get_inode %d\n", inum);

  if (inum == 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }
  /*
  bm->read_block(FIBBLOCK, buf);
  index = inum / 8;
  offset = inum % 8;
  mask = 0x80;
  mask = mask >> offset;
  if ((buf[index] & mask) == 0) {
    printf("\tim: invalid inode number\n");
    return NULL;
  }*/

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  char buf[BLOCK_SIZE];
  unsigned int fsize;
  inode_t *ino = get_inode(inum);

  if (ino == NULL) {
    printf("\tim: file for read not exist\n");
    return;
  }

  fsize = ino->size;
  if (fsize == 0)
    *buf_out = NULL;
  else {
    *buf_out = (char *) malloc(fsize);
    char idrct_blocks[BLOCK_SIZE];
    blockid_t *idblocks;
    int ful_blk, cur_blk, leftover, stop;

    ful_blk = fsize / BLOCK_SIZE;
    leftover = fsize % BLOCK_SIZE;
    stop = MIN(ful_blk, NDIRECT);

    for (cur_blk = 0; cur_blk < stop; ++cur_blk) {
      bm->read_block(ino->blocks[cur_blk], buf);
      // specified to BLOCK_SIZE
      memcpy(*buf_out + (cur_blk << 9), buf, BLOCK_SIZE);
    }

    if (cur_blk == NDIRECT) {
      bm->read_block(ino->blocks[NDIRECT], idrct_blocks);
      idblocks = (blockid_t *) idrct_blocks;

      for (; cur_blk < ful_blk; ++cur_blk) {
        bm->read_block(*(idblocks + (cur_blk - stop)), buf);
        memcpy(*buf_out + (cur_blk << 9), buf, BLOCK_SIZE);
      }

      if (leftover > 0) {
        bm->read_block(*(idblocks + (cur_blk - stop)), buf);
        memcpy(*buf_out + (cur_blk << 9), buf, leftover);
      }
    }
    else {
      if (leftover > 0) {
        bm->read_block(ino->blocks[cur_blk], buf);
        memcpy(*buf_out + (cur_blk << 9), buf, leftover);
      }
    }
  }

  *size = fsize;
  ino->atime = (unsigned int) time(NULL);
  put_inode(inum, ino);
  free(ino);
}

/* alloc/free blocks if needed */
// Consider all situations with regard to the value
// of nblk, org_blk, NDIRECT and offset
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  char buf_in[BLOCK_SIZE];
  unsigned int fsize;
  inode_t *ino = get_inode(inum);
  char idrct_blocks[BLOCK_SIZE];
  blockid_t *idblocks, new_blk;
  int nblk, org_nblk, offset, cur_blk, stop;

  if (size > static_cast<int>(MAXFILE * BLOCK_SIZE)) {
    printf("\tim: file to write exceeds size limit\n");
    exit(1);
  }
  if (ino == NULL) {
    printf("\tim: file not exist\n");
    return;
  }

  fsize = ino->size;
  idblocks = (blockid_t *) idrct_blocks;
  nblk = size / BLOCK_SIZE;
  offset = size % BLOCK_SIZE;
  org_nblk = fsize / BLOCK_SIZE;
  if (fsize % BLOCK_SIZE > 0)
    ++org_nblk;
  stop = MIN(org_nblk, nblk);
  stop = MIN(stop, NDIRECT);

  for (cur_blk = 0; cur_blk < stop; ++cur_blk) {
    // specified to BLOCK_SIZE
    memcpy(buf_in, buf + (cur_blk << 9), BLOCK_SIZE);
    bm->write_block(ino->blocks[cur_blk], buf_in);
  }

  if (stop == nblk && stop < org_nblk && stop < NDIRECT) {
    if (offset > 0) {
      memcpy(buf_in, buf + (cur_blk << 9), offset);
      bm->write_block(ino->blocks[cur_blk], buf_in);
      ++cur_blk;
    }
  }
  else if (stop == NDIRECT && stop < org_nblk) {
    bm->read_block(ino->blocks[cur_blk], idrct_blocks);
    stop = MIN(nblk, org_nblk);

    for (; cur_blk < stop; ++cur_blk) {
      memcpy(buf_in, buf + (cur_blk << 9), BLOCK_SIZE);
      bm->write_block(*(idblocks + (cur_blk - NDIRECT)), buf_in);
    }
    
    if (stop < org_nblk) {
      if (offset > 0) {
        memcpy(buf_in, buf + (cur_blk << 9), offset);
        bm->write_block(*(idblocks + (cur_blk - NDIRECT)), buf_in);
        ++cur_blk;
      }
    }
    else if (offset > 0 || nblk > org_nblk) {
      for (; cur_blk < nblk; ++cur_blk) {
        new_blk = bm->alloc_block();
        memcpy(buf_in, buf + (cur_blk << 9), BLOCK_SIZE);
        bm->write_block(new_blk, buf_in);
        *(idblocks + (cur_blk - NDIRECT)) = new_blk;
      }
      if (offset > 0) {
        new_blk = bm->alloc_block();
        memcpy(buf_in, buf + (cur_blk << 9), offset);
        bm->write_block(new_blk, buf_in);
        *(idblocks + (cur_blk - NDIRECT)) = new_blk;
        ++cur_blk;
      }

      bm->write_block(ino->blocks[NDIRECT], idrct_blocks);
    }
  }
  else if (stop == org_nblk) {
    stop = MIN(nblk, NDIRECT);

    for (; cur_blk < stop; ++cur_blk) {
      new_blk = bm->alloc_block();
      memcpy(buf_in, buf + (cur_blk << 9), BLOCK_SIZE);
      bm->write_block(new_blk, buf_in);
      ino->blocks[cur_blk] = new_blk;
    }

    if (stop == nblk && stop < NDIRECT) {
      if (offset > 0) {
        new_blk = bm->alloc_block();
        memcpy(buf_in, buf + (cur_blk << 9), offset);
        bm->write_block(new_blk, buf_in);
        ino->blocks[cur_blk] = new_blk;
        ++cur_blk;
      }
    }
    else if (offset > 0 || nblk > NDIRECT) {
      ino->blocks[NDIRECT] = bm->alloc_block();

      for (; cur_blk < nblk; ++cur_blk) {
        new_blk = bm->alloc_block();
        memcpy(buf_in, buf + (cur_blk << 9), BLOCK_SIZE);
        bm->write_block(new_blk, buf_in);
        *(idblocks + (cur_blk - NDIRECT)) = new_blk;
      }
      if (offset > 0) {
        new_blk = bm->alloc_block();
        memcpy(buf_in, buf + (cur_blk << 9), offset);
        bm->write_block(new_blk, buf_in);
        *(idblocks + (cur_blk - NDIRECT)) = new_blk;
        ++cur_blk;
      }

      bm->write_block(ino->blocks[NDIRECT], idrct_blocks);
    }
  }

  // finish write file
  // start to free block
  if (org_nblk > cur_blk) {
    stop = MIN(org_nblk, NDIRECT);

    for (; cur_blk < stop; ++cur_blk)
      bm->free_block(ino->blocks[cur_blk]);
    if (cur_blk < org_nblk) {
      if (cur_blk > NDIRECT) {
        for (; cur_blk < org_nblk; ++cur_blk)
          bm->free_block(*(idblocks + (cur_blk - NDIRECT)));
        bm->write_block(ino->blocks[NDIRECT], idrct_blocks);
      }
      else {
        bm->read_block(ino->blocks[NDIRECT], idrct_blocks);
        for (; cur_blk < org_nblk; ++cur_blk)
          bm->free_block(*(idblocks + (cur_blk - NDIRECT)));
        bm->free_block(ino->blocks[NDIRECT]);
      }
    }
  }

  ino->size = size;
  ino->atime = (unsigned int) time(NULL);
  ino->mtime = (unsigned int) time(NULL);
  put_inode(inum, ino);
  free(ino);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *ino = get_inode(inum);
  if (ino == NULL)
    return;

  a.type = (uint32_t) ino->type;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.size = ino->size;

  free(ino);
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode_t *ino;
  int nblk, offset, fsize;
  int cur_blk, stop;

  ino = get_inode(inum);
  if (ino == NULL) {
    printf("\tim: file not exist\n");
    return;
  }

  fsize = ino->size;
  nblk = fsize / BLOCK_SIZE;
  offset = fsize % BLOCK_SIZE;
  if (offset > 0)
    ++nblk;

  stop = MIN(nblk, NDIRECT);
  for (cur_blk = 0; cur_blk < stop; ++cur_blk)
    bm->free_block(ino->blocks[cur_blk]);
  if (stop < nblk) {
    char idrct_blocks[BLOCK_SIZE];
    blockid_t *idblocks;
    idblocks = (blockid_t *) idrct_blocks;

    bm->read_block(ino->blocks[NDIRECT], idrct_blocks);
    for (; cur_blk < nblk; ++cur_blk)
      bm->free_block(*(idblocks + (cur_blk - NDIRECT)));
    bm->free_block(ino->blocks[NDIRECT]);
  }

  free_inode(inum);
  free(ino);
}

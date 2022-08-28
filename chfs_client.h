#ifndef chfs_client_h
#define chfs_client_h

#include <string>
//#include "chfs_protocol.h"
#include "extent_client.h"
#include <vector>

const unsigned CHFS_NAME_LEN = 255;

struct chfs_dirent {
  uint32_t inum;
  uint16_t rec_len;
  uint8_t name_len;
  uint8_t file_type;
  char name[CHFS_NAME_LEN];
};

const unsigned CHFS_DIRENT_SIZE = 264;

class chfs_client {
  extent_client *ec;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    chfs_client::inum inum;
    dirent() = default;
    dirent(const std::string &s, chfs_client::inum i): name(s), inum(i) { }
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

 public:
  chfs_client();
  chfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int mkdir(inum , const char *, mode_t , inum &);
  
  /** you may need to add symbolic link related methods here.*/
};

#endif 

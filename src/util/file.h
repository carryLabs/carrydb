/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_FILE_H_
#define UTIL_FILE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <dirent.h>
#include "log.h"

static inline
bool file_exists(const std::string &filename){
	struct stat st;
	return stat(filename.c_str(), &st) == 0;
}

static inline
bool is_dir(const std::string &filename){
	struct stat st;
	if(stat(filename.c_str(), &st) == -1){
		return false;
	}
	return (bool)S_ISDIR(st.st_mode);
}

static inline
bool is_file(const std::string &filename){
	struct stat st;
	if(stat(filename.c_str(), &st) == -1){
		return false;
	}
	return (bool)S_ISREG(st.st_mode);
}

// return number of bytes read
static inline
int file_get_contents(const std::string &filename, std::string *content){
	char buf[8192];
	FILE *fp = fopen(filename.c_str(), "rb");
	if(!fp){
		return -1;
	}
	int ret = 0;
	while(!feof(fp) && !ferror(fp)){
		int n = fread(buf, 1, sizeof(buf), fp);
		if(n > 0){
			ret += n;
			content->append(buf, n);
		}
	}
	fclose(fp);
	return ret;
}

// return number of bytes written
static inline
int file_put_contents(const std::string &filename, const std::string &content){
	FILE *fp = fopen(filename.c_str(), "wb");
	if(!fp){
		return -1;
	}
	int ret = fwrite(content.data(), 1, content.size(), fp);
	fclose(fp);
	return ret == (int)content.size()? ret : -1;
}

class FileSpace
{
	public:
		FileSpace(const std::string &sFilePath) {
			nBytes = 0;
			checkSpace(sFilePath);
			addBytes(4096);
		}
		uint64_t getSpaceBytes() {
			return nBytes;
		}

	private:
		uint64_t nBytes = 0;
		void checkSpace(const std::string &path) {
			DIR *dp;
            struct dirent *entry;
            struct stat statbuf;
            if ((dp = opendir(path.data())) == NULL) {
                log_error("Cannot open dir: %s\n", path.data());
                return;
            }

            while ((entry = readdir(dp)) != NULL) {
                lstat(std::string(path + "/" + entry->d_name).data(), &statbuf);     
                if (S_ISDIR(statbuf.st_mode)) {
                    if (strncmp(".", entry->d_name, 1) == 0){ 
                        continue;
                    }   
                } else {
                    nBytes += statbuf.st_size;
                }
            }

            closedir(dp);
		}
		void addBytes(int bytes) {
			nBytes += bytes;
		}
};

static inline uint64_t Du(const std::string &filename) {
	struct stat statbuf;
	uint64_t sum;
	if (lstat(filename.c_str(), &statbuf) != 0) {
		return 0;
	}
	if (S_ISLNK(statbuf.st_mode) && stat(filename.c_str(), &statbuf) != 0) {
		return 0;
	}
	sum = statbuf.st_size;
	if (S_ISDIR(statbuf.st_mode)) {
		DIR *dir = NULL;
		struct dirent *entry;
		std::string newfile;

		dir = opendir(filename.c_str());
		if (!dir) {
			return sum;
		}
		while ((entry = readdir(dir))) {
			if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
				continue;
			}
			newfile = filename + "/" + entry->d_name;
			sum += Du(newfile);
		}
		closedir(dir);
	}
	return sum;
}

static inline  bool DirExists(const std::string& dname) {
	struct stat statbuf;
	if (stat(dname.c_str(), &statbuf) == 0) {
		return S_ISDIR(statbuf.st_mode);
	}
	return false; // stat() failed return false
}

static inline int CreateDir(const std::string& name){
	if (mkdir(name.c_str(), 0755) != 0) {
    	return -1;
    }
    return 1;
 };

static inline int CreateDirIfMissing(const std::string& name) {
	if (mkdir(name.c_str(), 0755) != 0) {
		if (errno != EEXIST) {
			log_error("mkdir name %s  error %d", name.c_str(), errno);

			return -1;
		} else if (!DirExists(name)) { // Check that name is actually a
			// directory.
			// Message is taken from mkdir
			log_error("name %s  exists but is not a directory", name.c_str());
			return -1;
		}
	}
	return 1;
}


#endif

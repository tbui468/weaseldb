#pragma once

#include <string>

#include "rocksdb/db.h"

namespace wsldb {

class Storage {
public:
    Storage(const std::string& db_path);
    void CreateIdxHandle(const std::string& idx_name);
    rocksdb::DB* GetIdxHandle(const std::string& idx_name);
    void DropIdxHandle(const std::string& idx_name);
    std::string PrependDBPath(const std::string& idx_name);
    inline rocksdb::DB* Catalogue() {
        return catalogue_handle_;
    }

private:
    rocksdb::DB* catalogue_handle_;
    std::vector<rocksdb::DB*> idx_handles_;
    rocksdb::Options options_;
    rocksdb::Status status_;
    std::string path_;
};

}

#pragma once

#include <string>
#include <unordered_map>
#include <memory>

#include "rocksdb/db.h"

namespace wsldb {

class Storage {
public:
    Storage(const std::string& db_path);
    void CreateIdxHandle(const std::string& idx_name);
    rocksdb::DB* GetIdxHandle(const std::string& idx_name);
    void DropIdxHandle(const std::string& idx_name);
    std::string PrependDBPath(const std::string& idx_name);
    inline std::string CataloguePath() const {
        return "";
    }
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

class Batch {
public:
    void Put(const std::string& db_name, const std::string& key, const std::string& value);
    void Delete(const std::string& db_name, const std::string& key);
    void Write(Storage& storage);
private:
    std::unordered_map<std::string, std::unique_ptr<rocksdb::WriteBatch>> batches_;
};

}

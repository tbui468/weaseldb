#pragma once

#include <string>
#include "rocksdb/db.h"
#include "token.h"
#include "status.h"

namespace wsldb {

class Stmt;

class DB {
public:
    DB(const std::string& path);
    virtual ~DB();
    std::vector<Token> tokenize(const std::string& query);
    std::vector<Stmt*> parse(const std::vector<Token>& tokens);
    wsldb::Status ExecuteScript(const std::string& path);

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

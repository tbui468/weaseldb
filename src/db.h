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
    rocksdb::Status execute(const std::string& query);
    wsldb::Status ExecuteScript(const std::string& path);
    void ShowTables();
    void AppendTableHandle(rocksdb::DB* h);
    rocksdb::DB* GetTableHandle(const std::string& table_name);
    std::string GetTablePath(const std::string& table_name);
    bool TableSchema(const std::string& table_name, std::string* serialized_schema);
    bool DropTable(const std::string& table_name);

    inline rocksdb::DB* Catalogue() {
        return catalogue_handle_;
    }

    inline std::string GetPath() {
        return path_;
    }

private:
    rocksdb::DB* catalogue_handle_;
    std::vector<rocksdb::DB*> table_handles_;
    rocksdb::Options options_;
    rocksdb::Status status_;
    std::string path_;
};

}

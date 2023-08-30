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

    inline rocksdb::DB* Catalogue() {
        return catalogue_handle_;
    }

    inline std::string GetPath() {
        return path_;
    }

    /*
    rocksdb::Status InsertRecord(int key, int v1, const std::string& v2) {
        std::string key_str((char*)&key, sizeof(key));
        std::string v1_str((char*)&v1, sizeof(int));
        status_ = db_->Put(rocksdb::WriteOptions(), key_str, v1_str + v2);
        return status_;
    }

    rocksdb::Status GetRecord(int key, WeaselRecord* rec) {
        std::string key_str((char*)&key, sizeof(key));
        std::string result;
        status_ = db_->Get(rocksdb::ReadOptions(), key_str, &result);
        if (!status_.ok()) {
            return status_;
        }
        rec->v1 = *((int*)(result.data()));
        rec->v2 = result.substr(sizeof(int), result.length() - sizeof(int));
        return status_;
    }

    rocksdb::Status DeleteRecord(int key) {
        std::string key_str((char*)&key, sizeof(key));
        status_ = db_->Delete(rocksdb::WriteOptions(), key_str);
        return status_;
    }

    std::vector<struct WeaselRecord> IterateScan() {
        it_ = db_->NewIterator(rocksdb::ReadOptions());
        struct WeaselRecord rec;
        std::vector<struct WeaselRecord> recs;

        for (it_->SeekToFirst(); it_->Valid(); it_->Next()) {
            std::string result = it_->value().ToString();
            rec.v1 = *((int*)(result.data()));
            rec.v2 = result.substr(sizeof(int), result.length() - sizeof(int));
            recs.push_back(rec);
        }

        delete it_;

        return recs;
    }*/

private:
    rocksdb::DB* catalogue_handle_;
    std::vector<rocksdb::DB*> table_handles_;
    rocksdb::Options options_;
    rocksdb::Status status_;
    std::string path_;
};

}

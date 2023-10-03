#pragma once

#include <string>
#include <unordered_map>
#include <memory>

#include "rocksdb/db.h"
#include "index.h"

namespace wsldb {

struct TableHandle {
    rocksdb::DB* db;
    std::vector<rocksdb::ColumnFamilyHandle*> cfs;
};

class Storage {
public:
    Storage(const std::string& db_path);
    void CreateTableHandle(const std::string& name, const std::vector<Index>& idxs);
    TableHandle& GetTableHandle(const std::string& name);
    void DropTableHandle(const std::string& name);

    std::string PrependDBPath(const std::string& idx_name);
    inline std::string CatalogueTableName() const {
        return "_catalogue";
    }

    static void DropDatabase(const std::string& db_path) {
        rocksdb::Options options;
        options.create_if_missing = false;
       
        //if database doesn't exist, just skip 
        rocksdb::DB* db;
        rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db);
        if (!s.ok())
            return;

        //get catalogue
        rocksdb::DB* catalogue;
        s = rocksdb::DB::Open(options, db_path + "/_catalogue", &catalogue);

        if (s.ok()) {
            //iterate through tables in catalogue and call DestroyDB on each one
            rocksdb::Iterator* it = catalogue->NewIterator(rocksdb::ReadOptions());
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                std::string table = it->key().ToString();
                rocksdb::DestroyDB(db_path + "/" + table, options);
            }

            delete catalogue;

            //call DestroyDB on catalogue
            rocksdb::DestroyDB(db_path + "/_catalogue", options);
        }

        delete db;

        s = rocksdb::DestroyDB(db_path, options);
        if (!s.ok())
            std::cout << "DestroyDB failed:" << s.getState() << std::endl;
    }

private:
    std::unordered_map<std::string, TableHandle> table_handles_;
    rocksdb::Options options_;
    rocksdb::Status status_;
    std::string path_;
};

class Batch {
public:
    void Put(const std::string& db_name, rocksdb::ColumnFamilyHandle* cfh, const std::string& key, const std::string& value);
    void Delete(const std::string& db_name, rocksdb::ColumnFamilyHandle* cfh, const std::string& key);
    void Write(Storage& storage);
private:
    std::unordered_map<std::string, std::unique_ptr<rocksdb::WriteBatch>> batches_;
};

}

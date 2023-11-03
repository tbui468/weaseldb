#pragma once

#include <string>
#include <unordered_map>
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "index.h"

namespace wsldb {

struct TableHandle {
    rocksdb::TransactionDB* db;
    std::vector<rocksdb::ColumnFamilyHandle*> cfs;
};

class Storage {
public:
    Storage(const std::string& db_path);
    virtual ~Storage();
    void CreateTable(const std::string& name, const std::vector<Index>& idxs);
    TableHandle GetTable(const std::string& name);
    void DropTable(const std::string& name);
    inline std::string CatalogueTableName() const { return "_catalogue"; }
    inline std::string PrependDBPath(const std::string& db_name) { return path_ + "/" + db_name; }
private:
    void OpenTable(const std::string& name);
    void CloseTable(const std::string& name);

public:
    static bool DatabaseExists(const std::string& db_path, rocksdb::DB** db) {
        rocksdb::Options options;
        options.create_if_missing = false;
       
        rocksdb::Status s = rocksdb::DB::Open(options, db_path, db);
        return s.ok();
    }

    static void DropDatabase(const std::string& db_path) {
        rocksdb::DB* db;
        if (!DatabaseExists(db_path, &db))
            return;

        rocksdb::Options options;

        //get catalogue
        rocksdb::DB* catalogue;
        rocksdb::Status s = rocksdb::DB::Open(options, db_path + "/_catalogue", &catalogue);

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
    std::string path_;
    rocksdb::Options options_;
    rocksdb::TransactionDBOptions txndb_options_;
    std::unordered_map<std::string, TableHandle> table_handles_;
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

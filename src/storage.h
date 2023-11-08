#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include <vector>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "index.h"
#include "status.h"
#include "schema.h"

namespace wsldb {

//TODO: need to replace other struct Txn with this one
class NewTxn {
public:
    NewTxn(rocksdb::Transaction* rocksdb_txn, 
           std::vector<rocksdb::ColumnFamilyDescriptor>* col_fam_descriptors, 
           std::vector<rocksdb::ColumnFamilyHandle*>* col_fam_handles): 
                rocksdb_txn_(rocksdb_txn),
                col_fam_descriptors_(col_fam_descriptors),
                col_fam_handles_(col_fam_handles) {}

    virtual ~NewTxn() { delete rocksdb_txn_; }

    Status Put(const std::string& col_fam, const std::string& key, const std::string& value) {
        rocksdb::Status s = rocksdb_txn_->Put(GetColFamHandle(col_fam), key, value);
        if (s.ok())
            return Status();
        return Status(false, "Execution Error: Rocksdb transaction Put failed");
    }

    Status Get(const std::string& col_fam, const std::string& key, std::string* value) {
        rocksdb::ReadOptions read_options;
        rocksdb::Status s = rocksdb_txn_->Get(read_options, GetColFamHandle(col_fam), key, value);
        if (s.ok())
            return Status();
        return Status(false, "Execution Error: Rocksdb transaction Get failed");
    }

    Status Delete(const std::string& col_fam, const std::string& key) {
        rocksdb::Status s = rocksdb_txn_->Delete(GetColFamHandle(col_fam), key);
        if (s.ok())
            return Status();
        return Status(false, "Execution Error: Rocksdb transaction Delete failed");
    }

    rocksdb::ColumnFamilyHandle* GetColFamHandle(const std::string& col_fam) {
        for (size_t i = 0; i < col_fam_descriptors_->size(); i++) {
            if (col_fam.compare(col_fam_descriptors_->at(i).name) == 0) {
                return col_fam_handles_->at(i);
            }
        }

        std::cout << "Invalid column family name\n";
        std::exit(1);
        return nullptr; //should never reach here
    }

    Status Commit() {
        rocksdb::Status s = rocksdb_txn_->Commit();
        if (s.ok()) {
            return Status();
        }
        return Status(false, "Execution Error: Rocksdb transaction commit failed");
    }
    Status Rollback() {
        rocksdb::Status s = rocksdb_txn_->Rollback();
        if (s.ok()) {
            return Status();
        }
        return Status(false, "Execution Error: Rocksdb transaction rollback failed");
    }
private:
    rocksdb::Transaction* rocksdb_txn_;
    std::vector<rocksdb::ColumnFamilyDescriptor>* col_fam_descriptors_;
    std::vector<rocksdb::ColumnFamilyHandle*>* col_fam_handles_;
};

class NewStorage {
public:
    NewStorage(const std::string& path): path_(path) {
        LoadColFamDescriptors();
        OpenDB();
    }

    virtual ~NewStorage() {
        delete db_;
    }

    void OpenDB() {
        rocksdb::Options options;
        options.create_if_missing = false;
        options.create_missing_column_families = true;
        rocksdb::TransactionDBOptions txndb_options;
        rocksdb::Status s = rocksdb::TransactionDB::Open(options, txndb_options, path_, col_fam_descriptors_, &col_fam_handles_, &db_);

        if (!s.ok()) {
            std::cout << "Failed to open rocksdb database\n";
            std::exit(1);
        }
    }

    void LoadColFamDescriptors() {
        rocksdb::Options options;
        options.create_if_missing = false;
        rocksdb::DB* db;
        rocksdb::Status s = rocksdb::DB::Open(options, path_, &db);

        if (!s.ok()) {
            std::cout << "Failed to open rocksdb database\n";
            std::exit(1);
        }

        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string table_name = it->key().ToString();
            std::string serialized_schema = it->value().ToString();
            Schema schema(table_name, serialized_schema);
            for (const Index& idx: schema.idxs_) {
                col_fam_descriptors_.emplace_back(idx.name_, rocksdb::ColumnFamilyOptions());
            }
        }

        delete db;
    }

    static void CreateDatabase(const std::string& path) {
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::TransactionDBOptions txndb_options;
        rocksdb::TransactionDB* db;
        rocksdb::Status s = rocksdb::TransactionDB::Open(options, txndb_options, path, &db);
        delete db;
    }
    static void DropDatabase(const std::string& path) {
        rocksdb::Options options;
        rocksdb::Status s = rocksdb::DestroyDB(path, options);
    }

    static std::string Catalog() {
        return rocksdb::kDefaultColumnFamilyName;
    }

    Status CreateTable(Schema* schema) {
        //TODO: obtain exclusive lock on db_;
   
        NewTxn* txn;
        BeginTxn(&txn);
        txn->Put(Catalog(), schema->table_name_, schema->Serialize());
        txn->Commit();
        delete txn;

        delete db_;
        col_fam_descriptors_.clear();
        LoadColFamDescriptors();
        OpenDB();

        //TODO: release exclusive lock on db_;
        return Status();
    }

    Status DropTable(Schema* schema) {
        //TODO: obtain exclusive lock on db_;

        NewTxn* txn;
        BeginTxn(&txn);
        txn->Delete(Catalog(), schema->table_name_);
        txn->Commit();
        delete txn;

        delete db_;
        col_fam_descriptors_.clear();
        LoadColFamDescriptors();
        OpenDB();

        //TODO: release exclusive lock on db_;
        return Status();
    }

    Status BeginTxn(NewTxn** txn) {
        rocksdb::WriteOptions options;
        rocksdb::Transaction* rocksdb_txn = db_->BeginTransaction(options);
        *txn = new NewTxn(rocksdb_txn, &col_fam_descriptors_, &col_fam_handles_);
        return Status();
    }
private:
    std::string path_;
    rocksdb::TransactionDB* db_;
    //Could use a hash map here to speed up mapping of column family names to column family handles
    std::vector<rocksdb::ColumnFamilyDescriptor> col_fam_descriptors_;
    std::vector<rocksdb::ColumnFamilyHandle*> col_fam_handles_;
};

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

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

class Iterator {
public:
    Iterator(rocksdb::Iterator* it): it_(it) {}
    virtual ~Iterator() { delete it_; }

    void Next() {
        it_->Next();
    }
    bool Valid() {
        return it_->Valid();
    }
    std::string Key() {
        return it_->key().ToString();
    }
    std::string Value() {
        return it_->value().ToString();
    }
    void SeekToFirst() {
        it_->SeekToFirst();
    }
private:
    rocksdb::Iterator* it_;
};

class Txn {
public:
    Txn(rocksdb::Transaction* rocksdb_txn, 
           std::vector<rocksdb::ColumnFamilyDescriptor>* col_fam_descriptors, 
           std::vector<rocksdb::ColumnFamilyHandle*>* col_fam_handles): 
                rocksdb_txn_(rocksdb_txn),
                col_fam_descriptors_(col_fam_descriptors),
                col_fam_handles_(col_fam_handles) {}

    virtual ~Txn() { delete rocksdb_txn_; }

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

    //TODO: Implement GetForUpdate (and use in Executor) for repeat-read/snapshot isolation level

    Status Delete(const std::string& col_fam, const std::string& key) {
        rocksdb::Status s = rocksdb_txn_->Delete(GetColFamHandle(col_fam), key);
        if (s.ok())
            return Status();
        return Status(false, "Execution Error: Rocksdb transaction Delete failed");
    }

    //TODO: Storage has the exact same function, Txn should get a function pointer to that function
    //rather than having its own version
    rocksdb::ColumnFamilyHandle* GetColFamHandle(const std::string& col_fam) {
        for (size_t i = 0; i < col_fam_descriptors_->size(); i++) {
            if (col_fam.compare(col_fam_descriptors_->at(i).name) == 0) {
                return col_fam_handles_->at(i);
            }
        }

        std::cout << "Txn::GetColFamHandle - Invalid column family name: " + col_fam + "\n";
        std::exit(1);
    }

    Status Commit() {
        rocksdb::Status s = rocksdb_txn_->Commit();
        if (!s.ok()) {
            std::cout << "Rocksdb Commit Error: " << s.ToString() << std::endl;
            std::exit(1);
        }

        return Status();
    }

    Status Rollback() {
        rocksdb::Status s = rocksdb_txn_->Rollback();
        if (!s.ok()) {
            std::cout << "Rocksdb Rollback Error: " << s.ToString() << std::endl;
            std::exit(1);
        }
        return Status();
    }
private:
    rocksdb::Transaction* rocksdb_txn_;
    std::vector<rocksdb::ColumnFamilyDescriptor>* col_fam_descriptors_;
    std::vector<rocksdb::ColumnFamilyHandle*>* col_fam_handles_;
};

class Storage {
public:
    Storage(const std::string& path): path_(path) {
        LoadColFamDescriptors();
        OpenDB();
    }

    virtual ~Storage() {
        for (rocksdb::ColumnFamilyHandle* handle: col_fam_handles_) {
            rocksdb::Status s = db_->DestroyColumnFamilyHandle(handle);
            if (!s.ok()) {
                std::cout << s.ToString() << std::endl;
                std::exit(1);
            }
        }
        delete db_;
    }

    void OpenDB() {
        rocksdb::Options options;
        options.create_if_missing = false;
        options.create_missing_column_families = true;
        rocksdb::TransactionDBOptions txndb_options;
        rocksdb::Status s = rocksdb::TransactionDB::Open(options, txndb_options, path_, col_fam_descriptors_, &col_fam_handles_, &db_);

        if (!s.ok()) {
            std::cout << "Rocksdb Error: " << s.ToString() << std::endl;
            std::exit(1);
        }
    }

    void LoadColFamDescriptors() {
        rocksdb::Options options;
        options.create_if_missing = false;
        rocksdb::DB* db;
        rocksdb::Status s = rocksdb::DB::Open(options, path_, &db);

        if (!s.ok()) {
            std::cout << "Rocksdb Error: " << s.ToString() << std::endl;
            std::exit(1);
        }

        col_fam_descriptors_.emplace_back(Catalog(), rocksdb::ColumnFamilyOptions());

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

    Status CreateTable(Schema* schema, Txn* txn) {
        //TODO: obtain exclusive lock on db_;

        txn->Put(Catalog(), schema->table_name_, schema->Serialize());

        for (const Index& idx: schema->idxs_) {
            rocksdb::ColumnFamilyHandle* cf;
            rocksdb::Status s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), idx.name_, &cf);
            if (!s.ok()) {
                std::cout << s.ToString() << std::endl;
                std::exit(1);
            }
            col_fam_handles_.push_back(cf);
            col_fam_descriptors_.emplace_back(idx.name_, rocksdb::ColumnFamilyOptions());
        }

        //TODO: release exclusive lock on db_;
        return Status();
    }

    Status DropTable(Schema* schema, Txn* txn) {
        //TODO: obtain exclusive lock on db_;


        txn->Delete(Catalog(), schema->table_name_);

        for (const Index& idx: schema->idxs_) {
            int i = GetColFamIdx(idx.name_);

            rocksdb::Status s = db_->DropColumnFamily(col_fam_handles_[i]);
            if (!s.ok()) {
                std::cout << s.ToString() << std::endl;
                std::exit(1);
            }

            s = db_->DestroyColumnFamilyHandle(col_fam_handles_[i]);
            if (!s.ok()) {
                std::cout << s.ToString() << std::endl;
                std::exit(1);
            }

            col_fam_handles_.erase(col_fam_handles_.begin() + i);
            col_fam_descriptors_.erase(col_fam_descriptors_.begin() + i);
        }



        //TODO: release exclusive lock on db_;
        return Status();
    }

    Txn* BeginTxn() {
        rocksdb::WriteOptions options;
        rocksdb::Transaction* rocksdb_txn = db_->BeginTransaction(options);
        return new Txn(rocksdb_txn, &col_fam_descriptors_, &col_fam_handles_);
    }

    Iterator* NewIterator(const std::string& col_fam) {
        return new Iterator(db_->NewIterator(rocksdb::ReadOptions(), GetColFamHandle(col_fam)));
    }

    int GetColFamIdx(const std::string& col_fam) {
        for (size_t i = 0; i < col_fam_descriptors_.size(); i++) {
            if (col_fam.compare(col_fam_descriptors_.at(i).name) == 0) {
                return i;
            }
        }

        std::cout << "GetColFamIdx - Invalid column family name: " + col_fam + "\n";
        std::exit(1);
    }

    //TODO: same function exists in Txn class - Txn class should just get a function pointer to this function
    rocksdb::ColumnFamilyHandle* GetColFamHandle(const std::string& col_fam) {
        for (size_t i = 0; i < col_fam_descriptors_.size(); i++) {
            if (col_fam.compare(col_fam_descriptors_.at(i).name) == 0) {
                return col_fam_handles_.at(i);
            }
        }

        std::cout << "GetColFamHandle - Invalid column family name: " + col_fam + "\n";
        std::exit(1);
    }

    Status GetSchema(const std::string& tab_name, std::string* serialized_schema) {
        rocksdb::ReadOptions read_options;
        rocksdb::Status s = db_->Get(read_options, tab_name, serialized_schema);
        if (!s.ok()) {
            return Status(false, "Table not found");
        }

        return Status();
    }
private:
    std::string path_;
    rocksdb::TransactionDB* db_;
    //Could use a hash map here to speed up mapping of column family names to column family handles
    std::vector<rocksdb::ColumnFamilyDescriptor> col_fam_descriptors_;
    std::vector<rocksdb::ColumnFamilyHandle*> col_fam_handles_;
};


}

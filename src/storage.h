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
#include "iterator.h"

namespace wsldb {

class Txn {
public:
    Txn(rocksdb::Transaction* rocksdb_txn, 
           std::vector<rocksdb::ColumnFamilyDescriptor>* col_fam_descriptors, 
           std::vector<rocksdb::ColumnFamilyHandle*>* col_fam_handles): 
                rocksdb_txn_(rocksdb_txn),
                col_fam_descriptors_(col_fam_descriptors),
                col_fam_handles_(col_fam_handles),
                has_aborted_(false) {}

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
public:
    bool has_aborted_;
};

class Storage {
public:
    Storage(const std::string& path);
    virtual ~Storage();
    void OpenDB();
    void LoadColFamDescriptors();
    static void CreateDatabase(const std::string& path);
    static void DropDatabase(const std::string& path);
    static std::string Catalog();
    Status CreateTable(Schema* schema, Txn* txn);
    Status DropTable(Schema* schema, Txn* txn);
    Txn* BeginTxn();
    Iterator* NewIterator(const std::string& col_fam);
    int GetColFamIdx(const std::string& col_fam);
    rocksdb::ColumnFamilyHandle* GetColFamHandle(const std::string& col_fam);
    Status GetSchema(const std::string& tab_name, std::string* serialized_schema);
private:
    std::string path_;
    rocksdb::TransactionDB* db_;
    //Could use a hash map here to speed up mapping of column family names to column family handles
    std::vector<rocksdb::ColumnFamilyDescriptor> col_fam_descriptors_;
    std::vector<rocksdb::ColumnFamilyHandle*> col_fam_handles_;
};


}

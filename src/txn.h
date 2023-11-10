#pragma once

#include <string>
#include <vector>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "status.h"

namespace wsldb {

class Txn {
public:
    Txn(rocksdb::Transaction* rocksdb_txn, 
           std::vector<rocksdb::ColumnFamilyDescriptor>* col_fam_descriptors, 
           std::vector<rocksdb::ColumnFamilyHandle*>* col_fam_handles);
    virtual ~Txn();
    Status Put(const std::string& col_fam, const std::string& key, const std::string& value);
    Status Get(const std::string& col_fam, const std::string& key, std::string* value);
    //TODO: Implement GetForUpdate (and use in Executor) for repeat-read/snapshot isolation level
    Status Delete(const std::string& col_fam, const std::string& key);
    //TODO: Storage has the exact same function, Txn should get a function pointer to that function
    //rather than having its own version
    rocksdb::ColumnFamilyHandle* GetColFamHandle(const std::string& col_fam);
    Status Commit();
    Status Rollback();
private:
    rocksdb::Transaction* rocksdb_txn_;
    std::vector<rocksdb::ColumnFamilyDescriptor>* col_fam_descriptors_;
    std::vector<rocksdb::ColumnFamilyHandle*>* col_fam_handles_;
public:
    bool has_aborted_;
};

}

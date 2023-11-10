#pragma once

#include <string>
#include <vector>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "status.h"
#include "schema.h"
#include "iterator.h"
#include "txn.h"

namespace wsldb {

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
    std::vector<rocksdb::ColumnFamilyDescriptor> col_fam_descriptors_;
    std::vector<rocksdb::ColumnFamilyHandle*> col_fam_handles_;
};


}

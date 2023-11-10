#include "txn.h"

namespace wsldb {
Txn::Txn(rocksdb::Transaction* rocksdb_txn, 
       std::vector<rocksdb::ColumnFamilyDescriptor>* col_fam_descriptors, 
       std::vector<rocksdb::ColumnFamilyHandle*>* col_fam_handles): 
            rocksdb_txn_(rocksdb_txn),
            col_fam_descriptors_(col_fam_descriptors),
            col_fam_handles_(col_fam_handles),
            has_aborted_(false) {}

Txn::~Txn() { delete rocksdb_txn_; }

Status Txn::Put(const std::string& col_fam, const std::string& key, const std::string& value) {
    rocksdb::Status s = rocksdb_txn_->Put(GetColFamHandle(col_fam), key, value);
    if (s.ok())
        return Status();
    return Status(false, "Execution Error: Rocksdb transaction Put failed");
}

Status Txn::Get(const std::string& col_fam, const std::string& key, std::string* value) {
    rocksdb::ReadOptions read_options;
    rocksdb::Status s = rocksdb_txn_->Get(read_options, GetColFamHandle(col_fam), key, value);
    if (s.ok())
        return Status();
    return Status(false, "Execution Error: Rocksdb transaction Get failed");
}

Status Txn::Delete(const std::string& col_fam, const std::string& key) {
    rocksdb::Status s = rocksdb_txn_->Delete(GetColFamHandle(col_fam), key);
    if (s.ok())
        return Status();
    return Status(false, "Execution Error: Rocksdb transaction Delete failed");
}

rocksdb::ColumnFamilyHandle* Txn::GetColFamHandle(const std::string& col_fam) {
    for (size_t i = 0; i < col_fam_descriptors_->size(); i++) {
        if (col_fam.compare(col_fam_descriptors_->at(i).name) == 0) {
            return col_fam_handles_->at(i);
        }
    }

    std::cout << "Txn::GetColFamHandle - Invalid column family name: " + col_fam + "\n";
    std::exit(1);
}

Status Txn::Commit() {
    rocksdb::Status s = rocksdb_txn_->Commit();
    if (!s.ok()) {
        std::cout << "Rocksdb Commit Error: " << s.ToString() << std::endl;
        std::exit(1);
    }

    return Status();
}

Status Txn::Rollback() {
    rocksdb::Status s = rocksdb_txn_->Rollback();
    if (!s.ok()) {
        std::cout << "Rocksdb Rollback Error: " << s.ToString() << std::endl;
        std::exit(1);
    }
    return Status();
}
}

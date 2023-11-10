#include <iostream>

#include "storage.h"

namespace wsldb {

Storage::Storage(const std::string& path): path_(path) {
    LoadColFamDescriptors();
    OpenDB();
}

Storage::~Storage() {
    for (rocksdb::ColumnFamilyHandle* handle: col_fam_handles_) {
        rocksdb::Status s = db_->DestroyColumnFamilyHandle(handle);
        if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            std::exit(1);
        }
    }
    delete db_;
}

void Storage::OpenDB() {
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

void Storage::LoadColFamDescriptors() {
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

void Storage::CreateDatabase(const std::string& path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::TransactionDBOptions txndb_options;
    rocksdb::TransactionDB* db;
    rocksdb::Status s = rocksdb::TransactionDB::Open(options, txndb_options, path, &db);
    delete db;
}
void Storage::DropDatabase(const std::string& path) {
    rocksdb::Options options;
    rocksdb::Status s = rocksdb::DestroyDB(path, options);
}

std::string Storage::Catalog() {
    return rocksdb::kDefaultColumnFamilyName;
}

Status Storage::CreateTable(Schema* schema, Txn* txn) {
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

Status Storage::DropTable(Schema* schema, Txn* txn) {
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

Txn* Storage::BeginTxn() {
    rocksdb::WriteOptions options;
    rocksdb::Transaction* rocksdb_txn = db_->BeginTransaction(options);
    return new Txn(rocksdb_txn, &col_fam_descriptors_, &col_fam_handles_);
}

Iterator* Storage::NewIterator(const std::string& col_fam) {
    return new Iterator(db_->NewIterator(rocksdb::ReadOptions(), GetColFamHandle(col_fam)));
}

int Storage::GetColFamIdx(const std::string& col_fam) {
    for (size_t i = 0; i < col_fam_descriptors_.size(); i++) {
        if (col_fam.compare(col_fam_descriptors_.at(i).name) == 0) {
            return i;
        }
    }

    std::cout << "GetColFamIdx - Invalid column family name: " + col_fam + "\n";
    std::exit(1);
}

//TODO: same function exists in Txn class - Txn class should just get a function pointer to this function
rocksdb::ColumnFamilyHandle* Storage::GetColFamHandle(const std::string& col_fam) {
    for (size_t i = 0; i < col_fam_descriptors_.size(); i++) {
        if (col_fam.compare(col_fam_descriptors_.at(i).name) == 0) {
            return col_fam_handles_.at(i);
        }
    }

    std::cout << "GetColFamHandle - Invalid column family name: " + col_fam + "\n";
    std::exit(1);
}

Status Storage::GetSchema(const std::string& tab_name, std::string* serialized_schema) {
    rocksdb::ReadOptions read_options;
    rocksdb::Status s = db_->Get(read_options, tab_name, serialized_schema);
    if (!s.ok()) {
        return Status(false, "Table not found");
    }

    return Status();
}


}

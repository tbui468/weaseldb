#include <iostream>

#include "storage.h"

namespace wsldb {

Storage::Storage(const std::string& path): path_(path) {
    options_.create_if_missing = true;

    CreateTable(CatalogueTableName(), {});

    //TODO: should open all table handles here
}

Storage::~Storage() {
    //TODO: close all table handles here
}

void Storage::CreateTable(const std::string& name, const std::vector<Index>& idxs) {
    rocksdb::Status s;
    rocksdb::TransactionDB* db;
    if (!(s = rocksdb::TransactionDB::Open(options_, txndb_options_, PrependDBPath(name), &db)).ok())
        std::cout << "rocksdb::DB::Open failed: " << s.getState() << std::endl;

    //default column family will store primary index
    for (size_t i = 1; i < idxs.size(); i++) {
        rocksdb::ColumnFamilyHandle* cf;

        if (!(s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), idxs.at(i).name_, &cf)).ok())
            std::cout << "rocksdb::DB::CreateColumnFamily failed: " << s.getState() << std::endl;

        db->DestroyColumnFamilyHandle(cf);
    }

    delete db;

    OpenTable(name);
}
TableHandle Storage::GetTable(const std::string& name) {
    if (table_handles_.find(name) == table_handles_.end()) {
        std::cout << "table handle '" << name << "' not found!\n";
    }
    return table_handles_.at(name);
}

void Storage::DropTable(const std::string& name) {
    CloseTable(name);

    rocksdb::DestroyDB(PrependDBPath(name), options_);
    table_handles_.erase(name);
}

void Storage::OpenTable(const std::string& name) {
    std::vector<std::string> col_family_names;
    rocksdb::DB::ListColumnFamilies(options_, PrependDBPath(name), &col_family_names);

    std::vector<rocksdb::ColumnFamilyDescriptor> cfds;

    for (const std::string& cf_name: col_family_names) {
        cfds.emplace_back(cf_name, rocksdb::ColumnFamilyOptions());
    }
    
    TableHandle handle;
    rocksdb::Status s = rocksdb::TransactionDB::Open(options_, txndb_options_, PrependDBPath(name), cfds, &handle.cfs, &handle.db);
    if (!s.ok())
        std::cout << "rocksdb::DB::Open failed: " << s.getState() << std::endl;

    table_handles_.insert({ name, handle });
}

void Storage::CloseTable(const std::string& name) {
    TableHandle handle = GetTable(name);

    for (rocksdb::ColumnFamilyHandle* cfh: handle.cfs) {
        handle.db->DestroyColumnFamilyHandle(cfh);
    }

    delete handle.db;
}

void Batch::Put(const std::string& db_name, rocksdb::ColumnFamilyHandle* cfh, const std::string& key, const std::string& value) {
    std::unordered_map<std::string, std::unique_ptr<rocksdb::WriteBatch>>::iterator it = batches_.find(db_name);

    if (it == batches_.end()) {
        batches_.insert({ db_name, std::make_unique<rocksdb::WriteBatch>() });        
        it = batches_.find(db_name);
    }

    it->second->Put(cfh, key, value);
}

void Batch::Delete(const std::string& db_name, rocksdb::ColumnFamilyHandle* cfh, const std::string& key) {
    std::unordered_map<std::string, std::unique_ptr<rocksdb::WriteBatch>>::iterator it = batches_.find(db_name);

    if (it == batches_.end()) {
        batches_.insert({ db_name, std::make_unique<rocksdb::WriteBatch>() });        
        it = batches_.find(db_name);
    }

    it->second->Delete(cfh, key);
}

void Batch::Write(Storage& storage) {
    for (const std::pair<const std::string, std::unique_ptr<rocksdb::WriteBatch>>& p: batches_) {
        TableHandle handle = storage.GetTable(p.first);
        handle.db->Write(rocksdb::WriteOptions(), p.second.get());
    }
}

}

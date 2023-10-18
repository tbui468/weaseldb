#include <iostream>

#include "storage.h"

namespace wsldb {

Storage::Storage(const std::string& path): path_(path) {
    //create database if it doesn't exist 
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db;
    rocksdb::Status s = rocksdb::DB::Open(options, path, &db);

    std::vector<rocksdb::ColumnFamilyDescriptor> cfds;
    cfds.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());

    TableHandle handle;
    s = rocksdb::DB::Open(options, PrependDBPath(CatalogueTableName()), cfds, &handle.cfs, &handle.db);

    table_handles_.insert({ CatalogueTableName(), handle });

    //TODO: should open all databases handles here
}

std::string Storage::PrependDBPath(const std::string& idx_name) {
    return path_ + "/" + idx_name;
}

void Storage::CreateTableHandle(const std::string& name, const std::vector<Index>& idxs) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status s;

    //create database and colunm families
    {
        rocksdb::DB* db;
        if (!(s = rocksdb::DB::Open(options, PrependDBPath(name), &db)).ok())
            std::cout << "rocksdb::DB::Open failed: " << s.getState() << std::endl;

        //default column family will store primary index
        for (size_t i = 1; i < idxs.size(); i++) {
            rocksdb::ColumnFamilyHandle* cf;

            if (!(s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), idxs.at(i).name_, &cf)).ok())
                std::cout << "rocksdb::DB::CreateColumnFamily failed: " << s.getState() << std::endl;

            db->DestroyColumnFamilyHandle(cf);
        }

        delete db;
    } 

    //open database with column families and put into table_handles_
    {
        std::vector<rocksdb::ColumnFamilyDescriptor> cfds;

        cfds.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());

        //default column family will store primary index
        for (size_t i = 1; i < idxs.size(); i++) {
            cfds.emplace_back(idxs.at(i).name_, rocksdb::ColumnFamilyOptions());
        }
        
        TableHandle handle;
        s = rocksdb::DB::Open(options, PrependDBPath(name), cfds, &handle.cfs, &handle.db);
        if (!s.ok())
            std::cout << "rocksdb::DB::Open failed: " << s.getState() << std::endl;

        table_handles_.insert({ name, handle });
    }
}

TableHandle& Storage::GetTableHandle(const std::string& name) {
    if (table_handles_.find(name) == table_handles_.end()) {
        std::cout << "table handle '" << name << "' not found!\n";
    }
    return table_handles_.at(name);
}

void Storage::DropTableHandle(const std::string& name) {
    TableHandle handle = GetTableHandle(name);

    //close database
    for (rocksdb::ColumnFamilyHandle* cfh: handle.cfs) {
        handle.db->DestroyColumnFamilyHandle(cfh);
    }

    delete handle.db;

    //delete database
    rocksdb::Options options;
    rocksdb::DestroyDB(PrependDBPath(name), options);

    table_handles_.erase(name);
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
        TableHandle handle = storage.GetTableHandle(p.first);
        handle.db->Write(rocksdb::WriteOptions(), p.second.get());
    }
}

}

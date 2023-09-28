#include <iostream>

#include "storage.h"

namespace wsldb {

Storage::Storage(const std::string& path): path_(path) {
    CreateIdxHandle("");
    catalogue_handle_ = GetIdxHandle("");

    //TODO: should open all databases handles here
}

std::string Storage::PrependDBPath(const std::string& idx_name) {
    return path_ + "/" + idx_name;
}

void Storage::CreateIdxHandle(const std::string& idx_name) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* idx_handle;
    rocksdb::Status status = rocksdb::DB::Open(options, PrependDBPath(idx_name), &idx_handle);
    idx_handles_.push_back(idx_handle);    
}

rocksdb::DB* Storage::GetIdxHandle(const std::string& idx_name) {
    for (rocksdb::DB* h: idx_handles_) {
        if (h->GetName().compare(PrependDBPath(idx_name)) == 0)
            return h;
    }

    /*
    //open idx if not found, and put into idx_handles_ vector
    rocksdb::Options options;
    options.create_if_missing = false;
    rocksdb::DB* idx_handle;
    rocksdb::Status status = rocksdb::DB::Open(options, PrependDBPath(idx_name), &idx_handle);
    idx_handles_.push_back(idx_handle);    

    return idx_handle;*/
    return nullptr;
}

void Storage::DropIdxHandle(const std::string& idx_name) {
    for (size_t i = 0; i < idx_handles_.size(); i++) {
        rocksdb::DB* h = idx_handles_.at(i);
        if (h->GetName().compare(PrependDBPath(idx_name)) == 0) {
            delete h;
            idx_handles_.erase(idx_handles_.begin() + i);
            break;
        }
    }

    rocksdb::Options options;
    rocksdb::DestroyDB(PrependDBPath(idx_name), options);
}

void Batch::Put(const std::string& db_name, const std::string& key, const std::string& value) {
    std::unordered_map<std::string, std::unique_ptr<rocksdb::WriteBatch>>::iterator it = batches_.find(db_name);
    if (it == batches_.end()) {
        batches_.insert({ db_name, std::make_unique<rocksdb::WriteBatch>() });        
        it = batches_.find(db_name);
    }

    it->second->Put(key, value);
}

void Batch::Delete(const std::string& db_name, const std::string& key) {
    std::unordered_map<std::string, std::unique_ptr<rocksdb::WriteBatch>>::iterator it = batches_.find(db_name);
    if (it == batches_.end()) {
        batches_.insert({ db_name, std::make_unique<rocksdb::WriteBatch>() });        
        it = batches_.find(db_name);
    }

    it->second->Delete(key);
}

void Batch::Write(Storage& storage) {
    for (const std::pair<const std::string, std::unique_ptr<rocksdb::WriteBatch>>& p: batches_) {
        rocksdb::DB* handle = storage.GetIdxHandle(p.first);
        handle->Write(rocksdb::WriteOptions(), p.second.get());
    }
}

}

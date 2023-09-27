#include <iostream>

#include "storage.h"

namespace wsldb {

Storage::Storage(const std::string& path): path_(path) {
    rocksdb::Options options_;
    options_.create_if_missing = true;
    status_ = rocksdb::DB::Open(options_, path, &catalogue_handle_);
    if (!status_.ok()) {
        std::cout << "Error:" << status_.ToString() << std::endl;
    }
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

    //open idx if not found, and put into idx_handles_ vector
    rocksdb::Options options;
    options.create_if_missing = false;
    rocksdb::DB* idx_handle;
    rocksdb::Status status = rocksdb::DB::Open(options, PrependDBPath(idx_name), &idx_handle);
    idx_handles_.push_back(idx_handle);    

    return idx_handle;
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


}

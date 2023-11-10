#pragma once


#include "rocksdb/db.h"

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

}

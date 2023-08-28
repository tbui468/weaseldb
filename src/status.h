#pragma once

#include "tuple.h"

namespace wsldb {

class Status {
public:
    Status(bool ok, std::string msg): ok_(ok), msg_(std::move(msg)), tuples_(nullptr) {}
    Status(bool ok, std::string msg, TupleSet* tuples): ok_(ok), msg_(std::move(msg)), tuples_(tuples) {}

    inline bool Ok() {
        return ok_;
    }

    const inline std::string& Msg() {
        return msg_;
    }

    inline TupleSet* Tuples() {
        return tuples_;
    }
private:
    bool ok_;
    std::string msg_;
    TupleSet* tuples_;
};

}

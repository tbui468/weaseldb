#pragma once

namespace wsldb {

class RowSet;

class Status {
public:
    Status(bool ok, std::string msg): ok_(ok), msg_(std::move(msg)), tuples_(nullptr) {}
    Status(bool ok, std::string msg, RowSet* tuples): ok_(ok), msg_(std::move(msg)), tuples_(tuples) {}

    inline bool Ok() {
        return ok_;
    }

    const inline std::string& Msg() {
        return msg_;
    }

    inline RowSet* Tuples() {
        return tuples_;
    }
private:
    bool ok_;
    std::string msg_;
    RowSet* tuples_;
};

}

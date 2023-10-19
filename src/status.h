#pragma once

#include <vector>

namespace wsldb {

class RowSet;

class Status {
public:
    Status(): ok_(true), msg_(""), rowsets_({}) {}
    Status(bool ok, std::string msg): ok_(ok), msg_(std::move(msg)), rowsets_({}) {}
    Status(bool ok, std::string msg, std::vector<RowSet*> rowsets): ok_(ok), msg_(std::move(msg)), rowsets_(rowsets) {}

    inline bool Ok() {
        return ok_;
    }

    const inline std::string& Msg() {
        return msg_;
    }

    inline std::vector<RowSet*> Tuples() {
        return rowsets_;
    }
private:
    bool ok_;
    std::string msg_;
    std::vector<RowSet*> rowsets_;
};

}

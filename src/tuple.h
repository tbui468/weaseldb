#pragma once

#include "datum.h"
#include "schema.h"

namespace wsldb {

class Tuple {
public:
    Tuple(std::vector<Datum> data): data(std::move(data)) {}
    std::vector<Datum> data;
};

class TupleSet {
public:
    TupleSet(std::vector<std::string> fields): fields(std::move(fields)) {}
    std::vector<std::string> fields;
    std::vector<Tuple*> tuples;
};


}

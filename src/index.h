#pragma once

#include <string>
#include <vector>
#include "datum.h"

namespace wsldb {


class Index {
public:
    Index(const std::string& name, std::vector<int> key_idxs): name_(name), key_idxs_(std::move(key_idxs)) {}
    Index(const std::string& buf, int* offset);
    std::string Serialize() const;
    std::string GetKeyFromFields(const std::vector<Datum>& data) const;
public:
    std::string name_;
    std::vector<int> key_idxs_;
};

}

#pragma once

#include <vector>
#include <string>

#include "token.h"
#include "datum.h"
#include "status.h"
#include "index.h"
#include "attribute.h"

namespace wsldb {

class Table {
public:
    Table(std::string name,
           std::vector<Token> names,
           std::vector<Token> types,
           std::vector<bool> not_null_constraints, 
           std::vector<std::vector<Token>> uniques);

    Table(std::string name, const std::string& buf);

    std::string Serialize() const;
    AttributeSet* MakeAttributeSet(const std::string& alias) const;
    std::string IdxName(const std::string& prefix, const std::vector<int>& idxs) const;
    int GetAttrIdx(const std::string& name) const;

    inline int64_t NextRowId() {
        return rowid_counter_++;
    }


public:
    std::string name_;
    int64_t rowid_counter_;
    std::vector<Attribute> attrs_;
    std::vector<bool> not_null_constraints_;
    std::vector<Index> idxs_;
};


}

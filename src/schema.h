#pragma once

#include <vector>
#include <string>

#include "token.h"
#include "datum.h"
#include "status.h"
#include "index.h"
#include "attribute.h"

namespace wsldb {

class Schema {
public:
    Schema(std::string table_name,
           std::vector<Token> names,
           std::vector<Token> types,
           std::vector<bool> not_null_constraints, 
           std::vector<std::vector<Token>> uniques);

    Schema(std::string table_name, const std::string& buf);

    std::string Serialize() const;
    std::vector<Datum> DeserializeData(const std::string& value) const;
    AttributeSet* MakeAttributeSet(const std::string& ref_name) const;
    std::string IdxName(const std::string& prefix, const std::vector<int>& idxs) const;
    int GetAttrIdx(const std::string& name) const;

    inline int64_t NextRowId() {
        return rowid_counter_++;
    }


public:
    std::string table_name_;
    int64_t rowid_counter_;
    std::vector<Attribute> attrs_;
    std::vector<Index> idxs_;
};


}

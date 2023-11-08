#pragma once

#include <vector>
#include <string>
#include <cassert>
#include <algorithm>

#include "token.h"
#include "datum.h"
#include "status.h"
#include "row.h"
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
    inline int64_t NextRowId() {
        return rowid_counter_++;
    }
    inline std::string IdxName(const std::string& prefix, const std::vector<int>& idxs) const {
        std::string result = prefix;

        for (int i: idxs) {
            result += "_" + attrs_.at(i).name;            
        }

        return result;
    }
    inline int GetAttrIdx(const std::string& name) const {
        for (size_t i = 0; i < attrs_.size(); i++) {
            if (name.compare(attrs_.at(i).name) == 0) {
                return i;
            }
        }

        return -1;
    }

    std::vector<Datum> DeserializeData(const std::string& value) {
        std::vector<Datum> data = std::vector<Datum>();
        int off = 0;
        for (const Attribute& a: attrs_) {
            data.push_back(Datum(value, &off, a.type));
        }

        return data;
    }

    AttributeSet* MakeAttributeSet(const std::string& ref_name) const {
        std::vector<std::string> names;
        std::vector<DatumType> types;
        std::vector<bool> not_nulls;

        for (const Attribute& a: attrs_) {
            names.push_back(a.name);
            types.push_back(a.type);
            not_nulls.push_back(a.not_null_constraint);
        }
                
        return new AttributeSet(ref_name, names, types, not_nulls);
    }
public:
    std::string table_name_;
    int64_t rowid_counter_;
    std::vector<Attribute> attrs_;
    std::vector<Index> idxs_;
};


}

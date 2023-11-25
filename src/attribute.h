#pragma once

#include <vector>
#include <algorithm>

#include "datum.h"
#include "status.h"

namespace wsldb {

struct Column;

struct Attribute {
    Attribute() {}
    Attribute(const std::string& rel_ref, const std::string& name, DatumType type, bool not_null_constraint):
        rel_ref(rel_ref), name(name), type(type), not_null_constraint(not_null_constraint) {}

    std::string rel_ref         {""};
    std::string name            {""};
    DatumType type              {DatumType::Null};
    bool not_null_constraint    {true};

    std::string ToString() const;
    Status CheckConstraints(DatumType type) const;
};

class AttributeSet {
public:
    AttributeSet(const std::string& ref_name, std::vector<std::string> names, std::vector<DatumType> types, std::vector<bool> not_nulls);
    AttributeSet(AttributeSet* left, AttributeSet* right, bool* has_duplicate_tables);
    AttributeSet(std::vector<Attribute> attrs): attrs_(attrs) {}
    Status ResolveColumnTable(Column* col);
    Status GetAttribute(Column* col, Attribute* result, int* idx);

    std::vector<Datum> DeserializeData(const std::string& value) const {
        std::vector<Datum> data = std::vector<Datum>();
        int off = 0;
        for (const Attribute& a: attrs_) {
            data.push_back(Datum(value, &off, a.type));
        }

        return data;
    }

    inline size_t AttributeCount() const {
        return attrs_.size();
    }

    inline std::vector<Attribute> GetAttributes() {
        return attrs_;
    }

private:
    std::vector<Attribute> attrs_;
};


}

#pragma once

#include <vector>
#include <algorithm>

#include "datum.h"
#include "status.h"

namespace wsldb {

struct Column;

struct Attribute {
    Attribute() {}
    Attribute(const std::string& rel_ref, const std::string& name, DatumType type):
        rel_ref(rel_ref), name(name), type(type) {}

    std::string rel_ref         {""};
    std::string name            {""};
    DatumType type              {DatumType::Null};

    std::string ToString() const;
};

class AttributeSet {
public:
    AttributeSet(AttributeSet* left, AttributeSet* right, bool* has_duplicate_tables);
    AttributeSet(std::vector<Attribute> attrs, std::vector<bool> not_nulls): attrs_(attrs), not_nulls_(not_nulls) {}
    Status ResolveColumnTable(Column* col);
    Status GetAttribute(Column* col, Attribute* result, int* idx);
    Status PassesConstraintChecks(Column* col, DatumType type);

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
    std::vector<bool> not_nulls_;
};


}

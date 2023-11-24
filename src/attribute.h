#pragma once

#include <vector>
#include <algorithm>

#include "datum.h"
#include "status.h"

namespace wsldb {

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
    bool Contains(const std::string& table, const std::string& col) const;
    Attribute GetAttribute(const std::string& table, const std::string& col) const;
    Status GetAttribute(const std::string& rel, const std::string& col, Attribute* result, int* idx) {
        std::vector<Attribute> results;
        int i = 0;
        for (const Attribute& a: attrs_) {
            if ((a.rel_ref.compare(rel) == 0 || rel == "") && a.name.compare(col) == 0) {
                results.push_back(a);
                *idx = i;
            }
            i++;
        }

        if (results.size() > 1) {
            return Status(false, "Error: Multiple columns with same name found");
        } else if (results.size() == 0) {
            return Status(false, "Error: Column not found");
        }

        *result = results.at(0);
        return Status();
    }
    int GetAttributeIdx(const std::string& table, const std::string& col) const;
    std::vector<std::string> TableNames() const;
    std::vector<Attribute>* TableAttributes(const std::string& table) const;

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

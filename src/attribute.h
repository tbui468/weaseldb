#pragma once

#include <vector>
#include <unordered_map>

#include "datum.h"
#include "status.h"

namespace wsldb {

struct Attribute {
    Attribute(): rel_ref(""), name(""), type(DatumType::Null), not_null_constraint(true) {} //placeholders
    Attribute(const std::string& rel_ref, const std::string& name, DatumType type, bool not_null_constraint):
        rel_ref(rel_ref), name(name), type(type), not_null_constraint(not_null_constraint) {}

    std::string rel_ref;
    std::string name;
    DatumType type;
    bool not_null_constraint;

    std::string ToString() const {
        std::string ret;

        ret += name + ",";
        ret += Datum::TypeToString(type) + ",";
        ret += not_null_constraint ? "true" : "false";

        return ret;
    }

    Status CheckConstraints(DatumType type) {
        if (not_null_constraint && type == DatumType::Null) {
            return Status(false, "Error: Value violates column 'not null' constraint");
        }

        if (type != DatumType::Null && type != this->type) {
            return Status(false, "Error: Value type does not match column type in table");
        }

        return Status(true, "ok");
    }
};

class AttributeSet {
public:
    AttributeSet(const std::string& ref_name, std::vector<std::string> names, std::vector<DatumType> types, std::vector<bool> not_nulls) {
        for (size_t i = 0; i < names.size(); i++) {
            attrs_.emplace_back(ref_name, names.at(i), types.at(i), not_nulls.at(i));
        }
    }

    AttributeSet(AttributeSet* left, AttributeSet* right, bool* has_duplicate_tables) {
        //TODO: not implementing has_duplicate_tables for now
        *has_duplicate_tables = false;

        attrs_.insert(attrs_.end(), left->attrs_.begin(), left->attrs_.end());
        attrs_.insert(attrs_.end(), right->attrs_.begin(), right->attrs_.end());
    }

    bool Contains(const std::string& table, const std::string& col) const {
        for (const Attribute& a: attrs_) {
            if (a.rel_ref.compare(table) == 0 && a.name.compare(col) == 0)
                return true;
        }

        return false;
    }

    Attribute GetAttribute(const std::string& table, const std::string& col) const {
        for (Attribute a: attrs_) {
            if (a.rel_ref.compare(table) == 0 && a.name.compare(col) == 0)
                return a;
        }

        return {}; //keep compiler quiet
    }

    int GetAttributeIdx(const std::string& table, const std::string& col) const {
        int i = 0;
        for (Attribute a: attrs_) {
            if (a.rel_ref.compare(table) == 0 && a.name.compare(col) == 0)
                return i;
            i++;
        }

        return i; //keep compiler quiet
    }

    std::vector<std::string> TableNames() const {
        std::vector<std::string> result;
        for (const Attribute& a: attrs_) {
            if (std::find(result.begin(), result.end(), a.rel_ref) == result.end())
                result.push_back(a.rel_ref);
        }

        return result;
    }

    std::vector<Attribute>* TableAttributes(const std::string& table) const {
        std::vector<Attribute>* result = new std::vector<Attribute>(); //TODO: should return a copy instead of pointer
        for (const Attribute& a: attrs_) {
            if (a.rel_ref.compare(table) == 0)
                result->push_back(a);
        }

        return result;
    }

    int AttributeCount() const {
        return attrs_.size();
    }

    std::vector<Attribute> GetAttributes() {
        return attrs_;
    }

private:
    std::vector<Attribute> attrs_;
};


}

#pragma once

#include <vector>
#include <unordered_map>

#include "datum.h"
#include "status.h"

namespace wsldb {

struct Attribute {
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
};

//WorkTables are (often) a bunch of physical tables concatenated together,
//WorkingAttribute includes offsets/table names associated with each attribute
//in case the same attribute name is used across multiple tables
struct WorkingAttribute: public Attribute {
    //dummy constructor to allow creation on stack (used in WorkingAttributeSet functions)
    WorkingAttribute(): Attribute("", "", DatumType::Null, true), scope(-1) {} //placeholders
    WorkingAttribute(Attribute a): Attribute(a.rel_ref, a.name, a.type, a.not_null_constraint), scope(-1) {} //scope is placeholder

    int scope; //default is -1.  Should be filled in with correct relative scope position during semantic analysis phase

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

class WorkingAttributeSet {
public:
    WorkingAttributeSet(const std::string& ref_name, std::vector<std::string> names, std::vector<DatumType> types, std::vector<bool> not_nulls) {
        for (size_t i = 0; i < names.size(); i++) {
            attrs_.emplace_back(Attribute(ref_name, names.at(i), types.at(i), not_nulls.at(i)));
        }
    }

    WorkingAttributeSet(WorkingAttributeSet* left, WorkingAttributeSet* right, bool* has_duplicate_tables) {
        //TODO: not implementing has_duplicate_tables for now
        *has_duplicate_tables = false;

        attrs_.insert(attrs_.end(), left->attrs_.begin(), left->attrs_.end());
        attrs_.insert(attrs_.end(), right->attrs_.begin(), right->attrs_.end());
    }

    bool Contains(const std::string& table, const std::string& col) const {
        for (const WorkingAttribute& a: attrs_) {
            if (a.rel_ref.compare(table) == 0 && a.name.compare(col) == 0)
                return true;
        }

        return false;
    }

    WorkingAttribute GetWorkingAttribute(const std::string& table, const std::string& col) const {
        for (WorkingAttribute a: attrs_) {
            if (a.rel_ref.compare(table) == 0 && a.name.compare(col) == 0)
                return a;
        }

        return {}; //keep compiler quiet
    }

    int GetWorkingAttributeIdx(const std::string& table, const std::string& col) const {
        int i = 0;
        for (WorkingAttribute a: attrs_) {
            if (a.rel_ref.compare(table) == 0 && a.name.compare(col) == 0)
                return i;
            i++;
        }

        return i; //keep compiler quiet
    }

    std::vector<std::string> TableNames() const {
        std::vector<std::string> result;
        for (const WorkingAttribute& a: attrs_) {
            if (std::find(result.begin(), result.end(), a.rel_ref) == result.end())
                result.push_back(a.rel_ref);
        }

        return result;
    }

    std::vector<WorkingAttribute>* TableWorkingAttributes(const std::string& table) const {
        std::vector<WorkingAttribute>* result = new std::vector<WorkingAttribute>(); //TODO: should return a copy instead of pointer
        for (const WorkingAttribute& a: attrs_) {
            if (a.rel_ref.compare(table) == 0)
                result->push_back(a);
        }

        return result;
    }

    int WorkingAttributeCount() const {
        return attrs_.size();
    }

    std::vector<Attribute> GetAttributes() const {
        std::vector<Attribute> result;
        for (const WorkingAttribute& a: attrs_) {
            result.emplace_back(a.rel_ref, a.name, a.type, a.not_null_constraint);
        }

        return result;
    }

private:
    std::vector<WorkingAttribute> attrs_;
};


}

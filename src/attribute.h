#pragma once

#include <vector>
#include <unordered_map>

#include "datum.h"
#include "status.h"

namespace wsldb {

struct Attribute {
    Attribute(const std::string& name, DatumType type, bool not_null_constraint):
        name(name), type(type), not_null_constraint(not_null_constraint) {}

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
    WorkingAttribute(): Attribute("", DatumType::Null, true), idx(-1), scope(-1) {} //placeholders
    WorkingAttribute(Attribute a, int idx): Attribute(a.name, a.type, a.not_null_constraint), idx(idx), scope(-1) {} //scope is placeholder

    int idx; //attributes of later tables are offset if multiple tables are joined into a single working table
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
        std::vector<WorkingAttribute>* attrs_vector = new std::vector<WorkingAttribute>();
        for (size_t i = 0; i < names.size(); i++) {
            attrs_vector->emplace_back(Attribute(names.at(i), types.at(i), not_nulls.at(i)), i + offset_);
        }

        attrs_.insert({ ref_name, attrs_vector });

        offset_ += names.size();
    }

    WorkingAttributeSet(WorkingAttributeSet* left, WorkingAttributeSet* right, bool* has_duplicate_tables) {
        for (const std::pair<const std::string, std::vector<WorkingAttribute>*>& p: left->attrs_) {
            if (right->attrs_.find(p.first) != right->attrs_.end()) {
                *has_duplicate_tables = true;
            }
        }

        attrs_.insert(left->attrs_.begin(), left->attrs_.end());
        attrs_.insert(right->attrs_.begin(), right->attrs_.end());

        //offset right attributes
        for (const std::pair<const std::string, std::vector<WorkingAttribute>*>& p: right->attrs_) {
            for (WorkingAttribute& a: *(p.second)) {
                a.idx += left->offset_;
            }
        }

        *has_duplicate_tables = false;
        offset_ += right->offset_;
    }

    bool Contains(const std::string& table, const std::string& col) const {
        if (attrs_.find(table) == attrs_.end())
            return false;

        for (const WorkingAttribute& a: *(attrs_.at(table))) {
            if (a.name.compare(col) == 0)
                return true;
        }

        return false;
    }

    WorkingAttribute GetWorkingAttribute(const std::string& table, const std::string& col) const {
        std::vector<WorkingAttribute>* v = attrs_.at(table);
        for (WorkingAttribute a: *v) {
            if (a.name.compare(col) == 0)
                return a;
        }

        return {}; //keep compiler quiet
    }

    std::vector<std::string> TableNames() const {
        std::vector<std::string> result;

        for (const std::pair<const std::string, std::vector<WorkingAttribute>*>& p: attrs_) {
            result.push_back(p.first);
        }

        return result;
    }

    std::vector<WorkingAttribute>* TableWorkingAttributes(const std::string& table) const {
        return attrs_.at(table);
    }

    int WorkingAttributeCount() const {
        int count = 0;
        for (const std::pair<const std::string, std::vector<WorkingAttribute>*>& p: attrs_) {
            count += p.second->size();
        }

        return count;
    }

    std::vector<Attribute> GetAttributes() const {
        std::vector<Attribute> ret;
        for (int i = 0; i < WorkingAttributeCount(); i++) {
            ret.emplace_back("", DatumType::Null, true); //placeholder just so that vector is correct size (probably a better way to do this)
        }

        for (const std::pair<const std::string, std::vector<WorkingAttribute>*>& p: attrs_) {
            for (const WorkingAttribute& a: *(p.second)) {
                ret.at(a.idx) = Attribute(a.name, a.type, true); //not_null_constraint no used, so arbitrarily setting it to true
            }
        }

        return ret;
    }

public:
    std::unordered_map<std::string, std::vector<WorkingAttribute>*> attrs_;
    int offset_ = 0; //used to offset Attributes when multiple tables are joined - can just use WorkingAttribute.idx directly 
};


}

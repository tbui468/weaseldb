#include "attribute.h"

namespace wsldb {

std::string Attribute::ToString() const {
    std::string ret;

    ret += name + ",";
    ret += Datum::TypeToString(type) + ",";
    ret += not_null_constraint ? "true" : "false";

    return ret;
}

Status Attribute::CheckConstraints(DatumType value_type) const {
    if (not_null_constraint && value_type == DatumType::Null) {
        return Status(false, "Error: Value violates column 'not null' constraint");
    }

    if (value_type != DatumType::Null && this->type != value_type && !Datum::CanCast(value_type, this->type)) {
        return Status(false, "Error: Value type does not match column type in table");
    }

    return Status(true, "ok");
}


AttributeSet::AttributeSet(const std::string& ref_name, std::vector<std::string> names, std::vector<DatumType> types, std::vector<bool> not_nulls) {
    for (size_t i = 0; i < names.size(); i++) {
        attrs_.emplace_back(ref_name, names.at(i), types.at(i), not_nulls.at(i));
    }
}

AttributeSet::AttributeSet(AttributeSet* left, AttributeSet* right, bool* has_duplicate_tables) {
    //TODO: not implementing has_duplicate_tables for now
    *has_duplicate_tables = false;

    attrs_.insert(attrs_.end(), left->attrs_.begin(), left->attrs_.end());
    attrs_.insert(attrs_.end(), right->attrs_.begin(), right->attrs_.end());
}

bool AttributeSet::Contains(const std::string& table, const std::string& col) const {
    for (const Attribute& a: attrs_) {
        if (a.rel_ref.compare(table) == 0 && a.name.compare(col) == 0)
            return true;
    }

    return false;
}

Attribute AttributeSet::GetAttribute(const std::string& table, const std::string& col) const {
    for (Attribute a: attrs_) {
        if (a.rel_ref.compare(table) == 0 && a.name.compare(col) == 0)
            return a;
    }

    return {}; //keep compiler quiet
}

int AttributeSet::GetAttributeIdx(const std::string& table, const std::string& col) const {
    int i = 0;
    for (Attribute a: attrs_) {
        if (a.rel_ref.compare(table) == 0 && a.name.compare(col) == 0)
            return i;
        i++;
    }

    return i; //keep compiler quiet
}

std::vector<std::string> AttributeSet::TableNames() const {
    std::vector<std::string> result;
    for (const Attribute& a: attrs_) {
        if (std::find(result.begin(), result.end(), a.rel_ref) == result.end())
            result.push_back(a.rel_ref);
    }

    return result;
}

std::vector<Attribute>* AttributeSet::TableAttributes(const std::string& table) const {
    std::vector<Attribute>* result = new std::vector<Attribute>(); //TODO: should return a copy instead of pointer
    for (const Attribute& a: attrs_) {
        if (a.rel_ref.compare(table) == 0)
            result->push_back(a);
    }

    return result;
}

}

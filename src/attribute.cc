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


}

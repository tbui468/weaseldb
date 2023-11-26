#include "attribute.h"
#include "expr.h"

namespace wsldb {

std::string Attribute::ToString() const {
    std::string ret;

    ret += name + ",";
    ret += Datum::TypeToString(type) + ",";

    return ret;
}


AttributeSet::AttributeSet(AttributeSet* left, AttributeSet* right, bool* has_duplicate_tables) {
    //TODO: not implementing has_duplicate_tables for now
    *has_duplicate_tables = false;

    attrs_.insert(attrs_.end(), left->attrs_.begin(), left->attrs_.end());
    attrs_.insert(attrs_.end(), right->attrs_.begin(), right->attrs_.end());

    not_nulls_.insert(not_nulls_.end(), left->not_nulls_.begin(), left->not_nulls_.end());
    not_nulls_.insert(not_nulls_.end(), right->not_nulls_.begin(), right->not_nulls_.end());
}

Status AttributeSet::ResolveColumnTable(Column* col) {
    if (col->table != "") return Status();

    int i = 0;
    for (const Attribute& a: attrs_) {
        if (a.name.compare(col->name) == 0) {
            col->table = a.rel_ref;
            i++;
        }
    }

    if (i < 1) {
        return Status(false, "Error: Column '" + col->name + "' not found");
    } else if (i > 1) {
        return Status(false, "Error: Multiple tables with the column name '" + col->name + "' found");
    }

    return Status();
}


Status AttributeSet::GetAttribute(Column* col, Attribute* result, int* idx) {
    std::vector<Attribute> results;
    int i = 0;
    for (const Attribute& a: attrs_) {
        if (a.rel_ref.compare(col->table) == 0 && a.name.compare(col->name) == 0) {
            results.push_back(a);
            *idx = i;
        }
        i++;
    }

    if (results.size() > 1) {
        return Status(false, "Error: Multiple tables with the column name '" + col->name + "' found");
    } else if (results.size() == 0) {
        return Status(false, "Error: Column '" + col->name + "' not found");
    }

    *result = results.at(0);
    return Status();
}

Status AttributeSet::PassesConstraintChecks(Column* col, DatumType type) {
    //Only checking not null constraint now
    if (type != DatumType::Null)
        return Status();

    int i = 0;
    for (const Attribute& a: attrs_) {
        if (a.rel_ref.compare(col->table) == 0 && a.name.compare(col->name) == 0) {
            if (not_nulls_.at(i)) 
                return Status(false, "Constraint Error: Value of '" + col->table + "." + col->name + "' cannot be null");
        }
        i++;
    }

    return Status();
}


}

#include "table.h"

namespace wsldb {

Table::Table(std::string name,
               std::vector<Token> names,
               std::vector<Token> types,
               std::vector<bool> not_null_constraints, 
               std::vector<std::vector<Token>> uniques) {

    name_ = name;
    rowid_counter_ = 0;

    //NOTE: index creation requires attrs_ to be filled in beforehand
    for (size_t i = 0; i < names.size(); i++) {
        attrs_.emplace_back(name_, names.at(i).lexeme, TypeTokenToDatumType(types.at(i).type), not_null_constraints.at(i));
    }

    //indexes
    for (const std::vector<Token>& col_group: uniques) {
        std::vector<int> idx_cols;
        for (Token t: col_group) {
            idx_cols.push_back(GetAttrIdx(t.lexeme));
        }

        idxs_.emplace_back(IdxName(name_, idx_cols), idx_cols);
    }
}

Table::Table(std::string name, const std::string& buf) {
    name_ = name;

    int off = 0; 

    rowid_counter_ = *((int64_t*)(buf.data() + off));
    off += sizeof(int64_t);

    //attributes
    int count = *((int*)(buf.data() + off));
    off += sizeof(int);

    for (int i = 0; i < count; i++) {
        DatumType type = *((DatumType*)(buf.data() + off));
        off += sizeof(DatumType);

        int str_size = *((int*)(buf.data() + off));
        off += sizeof(int);
        std::string attr_name = buf.substr(off, str_size);
        off += str_size;

        bool not_null_constraint = *((bool*)(buf.data() + off));
        off += sizeof(bool);

        attrs_.emplace_back(name_, attr_name, type, not_null_constraint);
    }

    //deserialize indexes
    int idx_count = *((int*)(buf.data() + off));
    off += sizeof(int);
    for (int i = 0; i < idx_count; i++) {
        idxs_.emplace_back(buf, &off);
    }
}

std::string Table::Serialize() const {
    std::string buf;

    buf.append((char*)&rowid_counter_, sizeof(int64_t));

    //attributes
    int count = attrs_.size();
    buf.append((char*)&count, sizeof(count));

    for (const Attribute& a: attrs_) {
        buf.append((char*)&a.type, sizeof(DatumType));
        int str_size = a.name.length();
        buf.append((char*)&str_size, sizeof(int));
        buf += a.name;
        buf.append((char*)&a.not_null_constraint, sizeof(bool));
    }

    //serialize indexes
    int idx_count = idxs_.size();
    buf.append((char*)&idx_count, sizeof(int));
    for (const Index& i: idxs_) {
        buf += i.Serialize();
    }

    return buf;
}

AttributeSet* Table::MakeAttributeSet(const std::string& alias) const {
    std::vector<std::string> names;
    std::vector<DatumType> types;
    std::vector<bool> not_nulls;

    for (const Attribute& a: attrs_) {
        names.push_back(a.name);
        types.push_back(a.type);
        not_nulls.push_back(a.not_null_constraint);
    }
            
    return new AttributeSet(alias, names, types, not_nulls);
}


std::string Table::IdxName(const std::string& prefix, const std::vector<int>& idxs) const {
    std::string result = prefix;

    for (int i: idxs) {
        result += "_" + attrs_.at(i).name;            
    }

    return result;
}

int Table::GetAttrIdx(const std::string& name) const {
    for (size_t i = 0; i < attrs_.size(); i++) {
        if (name.compare(attrs_.at(i).name) == 0) {
            return i;
        }
    }

    return -1;
}

}

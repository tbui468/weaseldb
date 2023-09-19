#include <algorithm>

#include "table.h"

namespace wsldb {


Table::Table(std::string table_name, std::vector<Token> names, std::vector<Token> types, std::vector<bool> not_null_constraints, std::vector<Token> primary_keys) {
    table_name_ = table_name;

    for (size_t i = 0; i < names.size(); i++) {
        attrs_.emplace_back(names.at(i).lexeme, types.at(i).type, not_null_constraints.at(i));
    }

    for (Token t: primary_keys) {
        pk_attr_idxs_.push_back(GetAttrIdx(t.lexeme));
    }

    rowid_counter_ = 0;
}

Table::Table(std::string table_name, const std::string& buf) {
    table_name_ = table_name;

    int off = 0; 

    rowid_counter_ = *((int64_t*)(buf.data() + off));
    off += sizeof(int64_t);

    //attributes
    int count = *((int*)(buf.data() + off));
    off += sizeof(int);

    for (int i = 0; i < count; i++) {
        TokenType type = *((TokenType*)(buf.data() + off));
        off += sizeof(TokenType);

        int str_size = *((int*)(buf.data() + off));
        off += sizeof(int);
        std::string name = buf.substr(off, str_size);
        off += str_size;

        bool not_null_constraint = *((bool*)(buf.data() + off));
        off += sizeof(bool);

        attrs_.emplace_back(name, type, not_null_constraint);
    }

    //primary key
    int pk_count = *((int*)(buf.data() + off));
    off += sizeof(int);

    for (int i = 0; i < pk_count; i++) {
        int k = *((int*)(buf.data() + off));
        off += sizeof(int);
        pk_attr_idxs_.push_back(k);
    }

}

std::string Table::Serialize() {
    std::string buf;

    buf.append((char*)&rowid_counter_, sizeof(int64_t));

    //attributes
    int count = attrs_.size();
    buf.append((char*)&count, sizeof(count));

    for (const Attribute& a: attrs_) {
        buf.append((char*)&a.type, sizeof(TokenType));
        int str_size = a.name.length();
        buf.append((char*)&str_size, sizeof(int));
        buf += a.name;
        buf.append((char*)&a.not_null_constraint, sizeof(bool));
    }

    //primary key(s)
    int pk_count = pk_attr_idxs_.size();
    buf.append((char*)&pk_count, sizeof(pk_count));
    for (int i: pk_attr_idxs_) {
        buf.append((char*)&i, sizeof(int));
    }

    return buf;
}

std::vector<Datum> Table::DeserializeData(const std::string& value) {
    std::vector<Datum> data = std::vector<Datum>();
    int off = 0;
    for (const Attribute& a: attrs_) {
        data.push_back(Datum(value, &off, a.type));
    }

    return data;
}

std::string Table::GetKeyFromData(const std::vector<Datum>& data) {
    std::string primary_key;
    for (int i: pk_attr_idxs_) {
        primary_key += data.at(i).Serialize();
    }
    return primary_key;
}

int Table::GetAttrIdx(const std::string& name) {
    for (size_t i = 0; i < attrs_.size(); i++) {
        if (name.compare(attrs_.at(i).name) == 0) {
            return i;
        }
    }

    return -1;
}

}

#include <algorithm>

#include "schema.h"

namespace wsldb {


Schema::Schema(std::string table_name, std::vector<Token> names, std::vector<Token> types, std::vector<Token> primary_keys) {
    table_name_ = table_name;
    for (Token name: names) {
        attr_names_.push_back(name.lexeme);
    }

    for (Token type: types) {
        attr_types_.push_back(type.type);
    }

    for (Token t: primary_keys) {
        pk_attr_idxs_.push_back(GetFieldIdx(t.lexeme));
    }

    rowid_counter_ = 0;
}

Schema::Schema(std::string table_name, const std::string& buf) {
    table_name_ = table_name;

    int off = 0; 

    rowid_counter_ = *((int64_t*)(buf.data() + off));
    off += sizeof(int64_t);

    //attributes
    int count = *((int*)(buf.data() + off));
    off += sizeof(int);

    for (int i = 0; i < count; i++) {
        attr_types_.push_back(*((TokenType*)(buf.data() + off)));
        off += sizeof(TokenType);

        int str_size = *((int*)(buf.data() + off));
        off += sizeof(int);
        attr_names_.push_back(buf.substr(off, str_size));
        off += str_size;
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

std::string Schema::Serialize() {
    std::string buf;

    buf.append((char*)&rowid_counter_, sizeof(int64_t));

    //attributes
    int count = attr_names_.size();
    buf.append((char*)&count, sizeof(count));

    for (size_t i = 0; i < attr_names_.size(); i++) { //types vector should be same size
        buf.append((char*)&attr_types_.at(i), sizeof(TokenType));
        int str_size = attr_names_.at(i).length();
        buf.append((char*)&str_size, sizeof(int));
        buf += attr_names_.at(i);
    }

    //primary key(s)
    int pk_count = pk_attr_idxs_.size();
    buf.append((char*)&pk_count, sizeof(pk_count));
    for (int i: pk_attr_idxs_) {
        buf.append((char*)&i, sizeof(int));
    }

    return buf;
}

std::vector<Datum> Schema::DeserializeData(const std::string& value) {
    std::vector<Datum> data = std::vector<Datum>();
    int off = 0;
    for (const TokenType& type: attr_types_) {
        data.push_back(Datum(value, &off, type));
    }

    return data;
}

std::string Schema::GetKeyFromData(const std::vector<Datum>& data) {
    std::string primary_key;
    for (int i: pk_attr_idxs_) {
        primary_key += data.at(i).Serialize();
    }
    return primary_key;
}

int Schema::GetFieldIdx(const std::string& name) {
    for (size_t i = 0; i < attr_names_.size(); i++) {
        if (name.compare(attr_names_.at(i)) == 0) {
            return i;
        }
    }

    return -1;
}

}

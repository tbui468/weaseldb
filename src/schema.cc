#include <algorithm>

#include "schema.h"

namespace wsldb {


Schema::Schema(std::vector<Attribute*> attributes, std::vector<Token> primary_keys) {
    for (Attribute* a: attributes) {
        attr_names_.push_back(a->name.lexeme);
        attr_types_.push_back(a->type.type);
    } 

    for (Token t: primary_keys) {
        pk_attr_idxs_.push_back(GetFieldIdx(t.lexeme));
    }
}

Schema::Schema(const std::string& buf) {
    int off = 0; 
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

    //attributes
    int count = attr_names_.size();
    buf.append((char*)&count, sizeof(count));

    for (int i = 0; i < attr_names_.size(); i++) { //types vector should be same size
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

std::string Schema::SerializeData(const std::vector<Datum>& data) {
    std::string value;
    for (Datum d: data) {
        value += d.ToString();
    }
    return value;
}

std::string Schema::GetKeyFromData(const std::vector<Datum>& data) {
    return data.at(pk_attr_idxs_.at(0)).ToString();
}

int Schema::GetFieldIdx(const std::string& name) {
    ptrdiff_t pos = std::distance(attr_names_.begin(), std::find(attr_names_.begin(), attr_names_.end(), name));

    if (pos >= attr_names_.size()) {
        return -1;
    }

    return pos;
}

}

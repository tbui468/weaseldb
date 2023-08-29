#include <algorithm>

#include "schema.h"

namespace wsldb {

Schema::Schema(const std::vector<Token>& names, const std::vector<Token>& types) {
    names_ = std::vector<std::string>();
    for (const Token& t: names) {
        names_.push_back(t.lexeme);
    }

    types_ = std::vector<TokenType>();
    for (Token t: types) {
        types_.push_back(t.type);
    }

    primary_key_idx_ = 0;
}

Schema::Schema(const std::string& buf) {
    names_ = std::vector<std::string>();
    types_ = std::vector<TokenType>();

    int off = 0; 
    int count = *((int*)(buf.data() + off));
    off += sizeof(int);

    for (int i = 0; i < count; i++) {
        types_.push_back(*((TokenType*)(buf.data() + off)));
        off += sizeof(TokenType);

        int str_size = *((int*)(buf.data() + off));
        off += sizeof(int);
        names_.push_back(buf.substr(off, str_size));
        off += str_size;
    }

    primary_key_idx_ = 0;
}

std::string Schema::Serialize() {
    std::string buf;

    int count = names_.size();
    buf.append((char*)&count, sizeof(count));

    for (int i = 0; i < names_.size(); i++) { //types vector should be same size
        buf.append((char*)&types_.at(i), sizeof(TokenType));
        int str_size = names_.at(i).length();
        buf.append((char*)&str_size, sizeof(int));
        buf += names_.at(i);
    }

    return buf;
}

std::vector<Datum> Schema::DeserializeData(const std::string& value) {
    std::vector<Datum> data = std::vector<Datum>();
    int off = 0;
    for (const TokenType& type: types_) {
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
    return data.at(primary_key_idx_).ToString();
}

int Schema::GetFieldIdx(const std::string& name) {
    ptrdiff_t pos = std::distance(names_.begin(), std::find(names_.begin(), names_.end(), name));

    if (pos >= names_.size()) {
        return -1;
    }

    return pos;
}

}

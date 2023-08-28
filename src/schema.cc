#include "schema.h"

namespace wsldb {

Schema::Schema(const std::vector<Token>& attrs, const std::vector<Token>& types) {
    primary_key_idx_ = 0;
    for (int i = 0; i < attrs.size(); i++) {
        fields_.push_back({attrs.at(i).lexeme, types.at(i).type});
    }
}

Schema::Schema(const std::string& buf) {
    primary_key_idx_ = 0;
    //deserialize to schema
    int off = 0; 
    int count = *((int*)(buf.data() + off));
    off += sizeof(int);

    for (int i = 0; i < count; i++) {
        Field f;
        f.type = *((TokenType*)(buf.data() + off));
        off += sizeof(TokenType);
        int str_size = *((int*)(buf.data() + off));
        off += sizeof(int);
        f.name = buf.substr(off, str_size);
        off += str_size;

        fields_.push_back(f);
    }
}

std::string Schema::Serialize() {
    std::string buf;

    int count = fields_.size();
    buf.append((char*)&count, sizeof(count));

    for (int i = 0; i < fields_.size(); i++) {
        buf.append((char*)&fields_.at(i).type, sizeof(TokenType));
        int str_size = fields_.at(i).name.length();
        buf.append((char*)&str_size, sizeof(int));
        buf += fields_.at(i).name;
    }

    return buf;
}

std::vector<Datum> Schema::DeserializeData(const std::string& value) {
    std::vector<Datum> data = std::vector<Datum>();
    int off = 0;
    for (const Field& f: fields_) {
        data.push_back(Datum(value, &off, f.type));
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

int Schema::PrimaryKeyIdx() {
    return primary_key_idx_;
}

}

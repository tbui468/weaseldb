#include "index.h"


namespace wsldb {

Index::Index(const std::string& buf, int* offset) {
    name_ = Datum(buf, offset, DatumType::Text).AsString();

    int count = Datum(buf, offset, DatumType::Int8).AsInt8();
    for (int i = 0; i < count; i++) {
        int idx = Datum(buf, offset, DatumType::Int8).AsInt8();
        key_idxs_.push_back(idx);
    }
}

std::string Index::Serialize() const {
    std::string result = Datum(name_).Serialize();

    int col_count = key_idxs_.size();
    result += Datum(col_count).Serialize();

    for (int i: key_idxs_) {
        result += Datum(i).Serialize();
    }

    return result;
}

}

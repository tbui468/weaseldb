#pragma once

#include <vector>
#include <string>
#include "token.h"
#include "expr.h"
#include "datum.h"

namespace wsldb {

struct Field {
    std::string name;
    TokenType type;
};

class Schema {
public:
    Schema(const std::vector<Token>& attrs, const std::vector<Token>& types);
    Schema(const std::string& buf);
    std::string Serialize();
    std::vector<Datum> DeserializeData(const std::string& value);
    std::string SerializeData(const std::vector<Datum>& data);
    std::string GetKeyFromData(const std::vector<Datum>& data);
    int PrimaryKeyIdx();
    inline Field At(int i) {
        return fields_.at(i);
    }

    inline int Count() {
        return fields_.size();
    }

private:
    std::vector<Field> fields_;
    int primary_key_idx_;
};

}

#pragma once

#include <vector>
#include <string>
#include "token.h"
#include "datum.h"

namespace wsldb {

class Schema {
public:
    Schema(const std::vector<Token>& attrs, const std::vector<Token>& types);
    Schema(const std::string& buf);
    std::string Serialize();
    std::vector<Datum> DeserializeData(const std::string& value);
    std::string SerializeData(const std::vector<Datum>& data);
    std::string GetKeyFromData(const std::vector<Datum>& data);
    int GetFieldIdx(const std::string& name);
    inline int FieldCount() {
        return names_.size(); // == types.size()
    }

private:
    std::vector<std::string> names_;
    std::vector<TokenType> types_;
    int primary_key_idx_;
};

}

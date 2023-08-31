#pragma once

#include <vector>
#include <string>
#include "token.h"
#include "datum.h"

namespace wsldb {

class Attribute {
public:
    Attribute(Token name, Token type): name(name), type(type) {}
    Token name;
    Token type;
};

/*
class ForeignKey: public Constraint {
public:
    ForeignKey(std::vector<Expr*> cols, Token foreign_table, std::vector<Expr*> foreign_cols):
        cols_(std::move(cols)), foreign_table_(foreign_table), foreign_cols_(std::move(foreign_cols)) {}
private:
    std::vector<Expr*> cols_;
    Token foreign_table_;
    std::vector<Expr*> foreign_cols_;
};*/

class Schema {
public:
    Schema(std::vector<Attribute*> attributes, std::vector<Token> primary_keys);
    Schema(const std::string& buf);
    std::string Serialize();
    std::vector<Datum> DeserializeData(const std::string& value);
    std::string SerializeData(const std::vector<Datum>& data);
    std::string GetKeyFromData(const std::vector<Datum>& data);
    int GetFieldIdx(const std::string& name);
    inline int FieldCount() {
        return attr_names_.size(); // == types.size()
    }
    inline TokenType Type(int i) {
        return attr_types_.at(i);
    }
    inline std::string Name(int i) {
        return attr_names_.at(i);
    }
    inline int PrimaryKeyCount() {
        return pk_attr_idxs_.size();
    }

    inline int PrimaryKey(int i) {
        return pk_attr_idxs_.at(i);
    }
private:
    std::vector<std::string> attr_names_;
    std::vector<TokenType> attr_types_;
    std::vector<int> pk_attr_idxs_;
    //std::vector<int> fk_attr_idxs_;
    //std::string fk_ref_;
};

}

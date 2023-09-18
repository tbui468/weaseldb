#pragma once

#include <vector>
#include <string>
#include <cassert>
#include "token.h"
#include "datum.h"
#include "status.h"

namespace wsldb {

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
    Schema(std::string table_name, std::vector<Token> names, std::vector<Token> types, std::vector<Token> primary_keys);
    Schema(std::string table_name, const std::string& buf);
    std::string Serialize();
    std::vector<Datum> DeserializeData(const std::string& value);
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
    inline std::string TableName() {
        return table_name_;
    }

    inline int64_t NextRowId() {
        return rowid_counter_++;
    }
private:
    std::string table_name_;
    std::vector<std::string> attr_names_;
    std::vector<TokenType> attr_types_;
    std::vector<int> pk_attr_idxs_;
    int64_t rowid_counter_;
    //std::vector<int> fk_attr_idxs_;
    //std::string fk_ref_;
};

struct Attribute {
    Attribute(): name(""), type(TokenType::Int4), idx(-1), scope(-1) {} //placeholders
    Attribute(std::string name, TokenType type, int idx): name(name), type(type), idx(idx), scope(-1) {}
    std::string name;
    TokenType type;
    int idx;
    int scope; //default is -1.  Should be filled in with correct relative scope position during semantic analysis phase

    Status CheckConstraints(TokenType type) {
        bool not_null = false; //placeholder until 'not null' constraint is implemented

        if (not_null && type == TokenType::Null) {
            return Status(false, "Error: Value violates column 'not null' constraint");
        }

        bool will_demote_int8_literal_later = this->type == TokenType::Int4 && type == TokenType::Int8;

        if (!will_demote_int8_literal_later && type != TokenType::Null && type != this->type) {
            return Status(false, "Error: Value type does not match column type in schema");
        }

        return Status(true, "ok");
    }


};

class AttributeSet {
public:
    AttributeSet(const std::string& ref_name, std::vector<std::string> names, std::vector<TokenType> types) {
        std::vector<Attribute>* attrs_vector = new std::vector<Attribute>();
        for (size_t i = 0; i < names.size(); i++) {
            attrs_vector->emplace_back(names.at(i), types.at(i), i + offset_);
        }

        attrs_.insert({ ref_name, attrs_vector });

        offset_ += names.size();
    }

    AttributeSet(Schema* schema, const std::string& ref_name) {
        std::vector<Attribute>* attrs_vector = new std::vector<Attribute>();
        for (int i = 0; i < schema->FieldCount(); i++) {
            attrs_vector->emplace_back(schema->Name(i), schema->Type(i), i + offset_);
        }

        attrs_.insert({ ref_name, attrs_vector });

        offset_ += schema->FieldCount();
    }

    AttributeSet(AttributeSet* left, AttributeSet* right, bool* has_duplicate_tables) {
        for (const std::pair<const std::string, std::vector<Attribute>*>& p: left->attrs_) {
            if (right->attrs_.find(p.first) != right->attrs_.end()) {
                *has_duplicate_tables = true;
            }
        }

        attrs_.insert(left->attrs_.begin(), left->attrs_.end());
        attrs_.insert(right->attrs_.begin(), right->attrs_.end());

        //offset right attributes
        for (const std::pair<const std::string, std::vector<Attribute>*>& p: right->attrs_) {
            for (Attribute& a: *(p.second)) {
                a.idx += left->offset_;
            }
        }

        *has_duplicate_tables = false;
        offset_ += right->offset_;
    }

    AttributeSet(Schema* schema): AttributeSet(schema, schema->TableName()) {}

    bool Contains(const std::string& table, const std::string& col) const {
        if (attrs_.find(table) == attrs_.end())
            return false;

        for (const Attribute& a: *(attrs_.at(table))) {
            if (a.name.compare(col) == 0)
                return true;
        }

        return false;
    }

    Attribute GetAttribute(const std::string& table, const std::string& col) const {
        std::vector<Attribute>* v = attrs_.at(table);
        for (Attribute a: *v) {
            if (a.name.compare(col) == 0)
                return a;
        }

        return {"", TokenType::Int4, 0}; //keep compiler quiet
    }

    std::vector<std::string> TableNames() const {
        std::vector<std::string> result;

        for (const std::pair<const std::string, std::vector<Attribute>*>& p: attrs_) {
            result.push_back(p.first);
        }

        return result;
    }

    std::vector<Attribute>* TableAttributes(const std::string& table) const {
        return attrs_.at(table);
    }

    int AttributeCount() const {
        int count = 0;
        for (const std::pair<const std::string, std::vector<Attribute>*>& p: attrs_) {
            count += p.second->size();
        }

        return count;
    }

public:
    std::unordered_map<std::string, std::vector<Attribute>*> attrs_;
    int offset_ = 0;
};

}

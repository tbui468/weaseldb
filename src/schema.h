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
private:
    std::string table_name_;
    std::vector<std::string> attr_names_;
    std::vector<TokenType> attr_types_;
    std::vector<int> pk_attr_idxs_;
    //std::vector<int> fk_attr_idxs_;
    //std::string fk_ref_;
};

struct Attribute {
    Attribute(std::string name, TokenType type, int idx): name(name), type(type), idx(idx) {}
    std::string name;
    TokenType type;
    int idx;
};

class AttributeSet {
public:
    void AppendAttributes(const std::string& ref_name, std::vector<std::string> names, std::vector<TokenType> types) {
        std::vector<Attribute>* attrs_vector = new std::vector<Attribute>();
        for (int i = 0; i < names.size(); i++) {
            attrs_vector->emplace_back(names.at(i), types.at(i), i + offset_);
        }

        if (attrs_.find(ref_name) != attrs_.end()) {
            std::cout << "table reference already exists\n";
        }

        attrs_.insert({ ref_name, attrs_vector });

        offset_ += names.size();
    }

    void AppendSchema(Schema* schema, const std::string& ref_name) {
        std::vector<Attribute>* attrs_vector = new std::vector<Attribute>();
        for (int i = 0; i < schema->FieldCount(); i++) {
            attrs_vector->emplace_back(schema->Name(i), schema->Type(i), i + offset_);
        }

        if (attrs_.find(ref_name) != attrs_.end()) {
            std::cout << "table reference already exists\n";
        }

        attrs_.insert({ ref_name, attrs_vector });

        offset_ += schema->FieldCount();
    }

    void AppendSchema(Schema* schema) {
        AppendSchema(schema, schema->TableName());
    }

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

    static Status Concatenate(AttributeSet** result, AttributeSet* left, AttributeSet* right) {
        *result = new AttributeSet();

        for (const std::pair<const std::string, std::vector<Attribute>*>& p: left->attrs_) {
            if (right->attrs_.find(p.first) != right->attrs_.end()) {
                return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
            }
        }

        (*result)->attrs_.insert(left->attrs_.begin(), left->attrs_.end());
        (*result)->attrs_.insert(right->attrs_.begin(), right->attrs_.end());

        //offset right attributes
        for (const std::pair<const std::string, std::vector<Attribute>*>& p: right->attrs_) {
            for (Attribute& a: *(p.second)) {
                a.idx += left->offset_;
            }
        }

        return Status(true, "ok");
    };

public:
    std::unordered_map<std::string, std::vector<Attribute>*> attrs_;
    int offset_ = 0;
};

}

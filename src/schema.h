#pragma once

#include <vector>
#include <string>
#include <cassert>
#include "token.h"
#include "datum.h"

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
    void AppendSchema(Schema* schema, const std::string& table_alias) {
        std::vector<Attribute>* attrs_vector = new std::vector<Attribute>();
        for (int i = 0; i < schema->FieldCount(); i++) {
            attrs_vector->emplace_back(schema->Name(i), schema->Type(i), i + offset_);
        }
        attrs_.insert({ schema->TableName(), attrs_vector });

        //mapping both alias and physical table name to physical table name
        //so we don't have to check for existence of key translating
        //eg, both of the following mappings work:
        //  alias->physical
        //  physical->physical
        aliases_.insert({ table_alias, schema->TableName() });
        aliases_.insert({ schema->TableName(), schema->TableName() });

        offset_ += schema->FieldCount();
    }

    void AppendSchema(Schema* schema) {
        AppendSchema(schema, schema->TableName());
    }

    bool Contains(const std::string& name, const std::string& col) const {
        std::string table = AliasToTable(name);
        if (attrs_.find(table) == attrs_.end())
            return false;

        for (const Attribute& a: *(attrs_.at(table))) {
            if (a.name.compare(col) == 0)
                return true;
        }

        return false;
    }

    Attribute GetAttribute(const std::string& table, const std::string& col) const {
        assert(Contains(AliasToTable(table), col) && "check that attribute exists before retreiving it");

        std::vector<Attribute>* v = attrs_.at(AliasToTable(table));
        for (Attribute a: *v) {
            if (a.name.compare(col) == 0)
                return a;
        }

        return {"", TokenType::Int4, 0}; //keep compiler quiet
    }

    //TODO: caller can just get list of tables with 'TableNames', and use 'Contains' function to check for uniqueness
    //This is really a composite of those two functions and can be removed
    std::vector<std::string> FindUniqueTablesWithCol(const std::string& col) const {
        std::vector<std::string> result;

        for (const std::pair<const std::string, std::vector<Attribute>*>& p: attrs_) {
            for (Attribute a: *(p.second)) {
                if (a.name.compare(col) == 0)
                    result.push_back(p.first);
            }
        }

        return result;
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

private:
    inline std::string AliasToTable(const std::string& alias) const {
        return aliases_.at(alias);
    }

private:
    std::unordered_map<std::string, std::vector<Attribute>*> attrs_;
    //mapping of aliases -> physical table names
    //includes mapping of physical table name -> physical table name
    //see AppendSchema function
    std::unordered_map<std::string, std::string> aliases_;
    int offset_ = 0;
};

}

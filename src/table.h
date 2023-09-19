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

struct Attribute {
    Attribute(const std::string& name, TokenType type, bool not_null_constraint):
        name(name), type(type), not_null_constraint(not_null_constraint) {}

    std::string name;
    TokenType type;
    bool not_null_constraint;
};

class Index {
    std::string name_;
    std::vector<int> key_idxs_;
};

class Table {
public:
    Table(std::string table_name, std::vector<Token> names, std::vector<Token> types, std::vector<bool> not_null_constraints, std::vector<Token> primary_keys);
    Table(std::string table_name, const std::string& buf);

    std::string Serialize();
    std::vector<Datum> DeserializeData(const std::string& value);
    std::string GetKeyFromData(const std::vector<Datum>& data);
    int GetAttrIdx(const std::string& name);

    inline const std::vector<Attribute>& Attrs() const {
        return attrs_;
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
    std::vector<Attribute> attrs_;
    std::vector<int> pk_attr_idxs_; //store this information in index metadata
    int64_t rowid_counter_;
    //std::vector<IndexMeta> idxs_; //primary index is in first position in vector
    //std::vector<int> fk_attr_idxs_;
    //std::string fk_ref_;
};

struct WorkingAttribute: public Attribute {
    //dummy constructor to allow creation on stack (used in WorkingAttributeSet functions)
    WorkingAttribute(): Attribute("", TokenType::Int4, true), idx(-1), scope(-1) {} //placeholders
    WorkingAttribute(Attribute a, int idx): Attribute(a.name, a.type, a.not_null_constraint), idx(idx), scope(-1) {} //scope is placeholder

    int idx; //attributes of later tables are offset if multiple tables are joined into a single working table
    int scope; //default is -1.  Should be filled in with correct relative scope position during semantic analysis phase

    Status CheckConstraints(TokenType type) {
        if (not_null_constraint && type == TokenType::Null) {
            return Status(false, "Error: Value violates column 'not null' constraint");
        }

        bool will_demote_int8_literal_later = this->type == TokenType::Int4 && type == TokenType::Int8;

        if (!will_demote_int8_literal_later && type != TokenType::Null && type != this->type) {
            return Status(false, "Error: Value type does not match column type in table");
        }

        return Status(true, "ok");
    }


};

class WorkingAttributeSet {
public:
    //This constructor is only used for ConstantTables, since a Physical table will use a table
    WorkingAttributeSet(const std::string& ref_name, std::vector<std::string> names, std::vector<TokenType> types) {
        std::vector<WorkingAttribute>* attrs_vector = new std::vector<WorkingAttribute>();
        for (size_t i = 0; i < names.size(); i++) {
            //not_null_constraint not used in ConstantTables, so just setting it to true
            attrs_vector->emplace_back(Attribute(names.at(i), types.at(i), true), i + offset_);
        }

        attrs_.insert({ ref_name, attrs_vector });

        offset_ += names.size();
    }

    WorkingAttributeSet(Table* table, const std::string& ref_name) {
        std::vector<WorkingAttribute>* attrs_vector = new std::vector<WorkingAttribute>();
        for (size_t i = 0; i < table->Attrs().size(); i++) {
            attrs_vector->emplace_back(table->Attrs().at(i), i + offset_);
        }

        attrs_.insert({ ref_name, attrs_vector });

        offset_ += table->Attrs().size();
    }

    WorkingAttributeSet(WorkingAttributeSet* left, WorkingAttributeSet* right, bool* has_duplicate_tables) {
        for (const std::pair<const std::string, std::vector<WorkingAttribute>*>& p: left->attrs_) {
            if (right->attrs_.find(p.first) != right->attrs_.end()) {
                *has_duplicate_tables = true;
            }
        }

        attrs_.insert(left->attrs_.begin(), left->attrs_.end());
        attrs_.insert(right->attrs_.begin(), right->attrs_.end());

        //offset right attributes
        for (const std::pair<const std::string, std::vector<WorkingAttribute>*>& p: right->attrs_) {
            for (WorkingAttribute& a: *(p.second)) {
                a.idx += left->offset_;
            }
        }

        *has_duplicate_tables = false;
        offset_ += right->offset_;
    }

    WorkingAttributeSet(Table* table): WorkingAttributeSet(table, table->TableName()) {}

    bool Contains(const std::string& table, const std::string& col) const {
        if (attrs_.find(table) == attrs_.end())
            return false;

        for (const WorkingAttribute& a: *(attrs_.at(table))) {
            if (a.name.compare(col) == 0)
                return true;
        }

        return false;
    }

    WorkingAttribute GetWorkingAttribute(const std::string& table, const std::string& col) const {
        std::vector<WorkingAttribute>* v = attrs_.at(table);
        for (WorkingAttribute a: *v) {
            if (a.name.compare(col) == 0)
                return a;
        }

        return {}; //keep compiler quiet
    }

    std::vector<std::string> TableNames() const {
        std::vector<std::string> result;

        for (const std::pair<const std::string, std::vector<WorkingAttribute>*>& p: attrs_) {
            result.push_back(p.first);
        }

        return result;
    }

    std::vector<WorkingAttribute>* TableWorkingAttributes(const std::string& table) const {
        return attrs_.at(table);
    }

    int WorkingAttributeCount() const {
        int count = 0;
        for (const std::pair<const std::string, std::vector<WorkingAttribute>*>& p: attrs_) {
            count += p.second->size();
        }

        return count;
    }

public:
    std::unordered_map<std::string, std::vector<WorkingAttribute>*> attrs_;
    int offset_ = 0;
};

}

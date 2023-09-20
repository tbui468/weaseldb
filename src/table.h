#pragma once

#include <vector>
#include <string>
#include <cassert>

#include "token.h"
#include "datum.h"
#include "status.h"
#include "db.h"
#include "row.h"
#include "rocksdb/db.h"

namespace wsldb {

struct Attribute {
    Attribute(const std::string& name, TokenType type, bool not_null_constraint):
        name(name), type(type), not_null_constraint(not_null_constraint) {}

    std::string name;
    TokenType type;
    bool not_null_constraint;
};

class Index {
public:
    Index(const std::string& name, std::vector<int> key_idxs): name_(name), key_idxs_(std::move(key_idxs)) {}
    Index(const std::string& buf, int* offset);
    Index(): name_(""), key_idxs_({}) {} //Default constructor so that Index can be created/copied in Table constructor - this is ugly
    std::string Serialize() const;
public:
    std::string name_;
    std::vector<int> key_idxs_;
};

class Table {
public:
    Table(std::string table_name, std::vector<Token> names, std::vector<Token> types, std::vector<bool> not_null_constraints, std::vector<Token> primary_keys);
    Table(std::string table_name, const std::string& buf);
    std::string Serialize();
    std::vector<Datum> DeserializeData(const std::string& value);
    //TODO: Generalize to work with primary and secondary idxs
    std::string GetKeyFromData(const std::vector<Datum>& data);
    int GetAttrIdx(const std::string& name);
    Status BeginScan(DB* db);
    Status NextRow(DB* db, Row** r);
    Status DeletePrev(DB* db);
    Status UpdatePrev(DB* db, Row* r);
    Status Insert(DB* db, std::vector<Datum>& data);

    inline const std::vector<Attribute>& Attrs() const {
        return attrs_;
    }
    inline std::vector<Datum> PrimaryKeyFields() const {
        std::vector<Datum> pk_data;
        std::string s = "Primary keys:";
        pk_data.push_back(Datum(s));
        for (size_t i = 0; i < primary_idx_.key_idxs_.size(); i++) {
            std::string name = Attrs().at(primary_idx_.key_idxs_.at(i)).name;
            pk_data.push_back(Datum(name));
        }
        return pk_data;
    }
    inline std::string TableName() {
        return table_name_;
    }
    inline int64_t NextRowId() {
        return rowid_counter_++;
    }
    inline std::string PrimaryIdxName() {
        return PrimaryIdxName(table_name_); 
    }
public:
    static std::string PrimaryIdxName(const std::string& table_name) {
        return table_name + "_primary";
    }
    static std::string SecondaryIdxName(const std::string& table_name, const std::vector<Attribute>& attrs, const std::vector<int>& idxs) {
        std::string result = table_name + "_secondary";

        for (int i: idxs) {
            result += "_" + attrs.at(i).name;            
        }

        return result;
    }
private:
    std::string table_name_;
    std::vector<Attribute> attrs_;
    int64_t rowid_counter_;
    rocksdb::Iterator* it_;
    Index primary_idx_;
    std::vector<Index> secondary_idxs_;
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

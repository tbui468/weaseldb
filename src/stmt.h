#pragma once

#include <iostream>
#include <unordered_map>
#include <limits>

#include "expr.h"
#include "token.h"
#include "table.h"
#include "status.h"
#include "rocksdb/db.h"

namespace wsldb {

//Moved class Stmt declaration to expr.h since we need it there,
//but including stmt.h would cause a circular dependency
enum class StmtType {
    Create,
    Insert,
    Update,
    Delete,
    Select,
    DescribeTable,
    DropTable
};

//Putting class Stmt here since we need it in Expr,
//but putting it in stmt.h would cause a circular dependency
class Stmt {
public:
    virtual StmtType Type() const = 0;
};

struct Txn {
    Txn(): stmts({}), commit_on_success(true) {}
    Txn(std::vector<Stmt*> stmts, bool commit_on_success): stmts(std::move(stmts)), commit_on_success(commit_on_success) {}
    std::vector<Stmt*> stmts;
    bool commit_on_success;
};

class CreateStmt: public Stmt {
public:
    CreateStmt(Token target, 
               std::vector<Token> names, 
               std::vector<Token> types, 
               std::vector<bool> not_null_constraints, 
               std::vector<Token> primary_keys,
               std::vector<std::vector<Token>> uniques,
               std::vector<bool> nulls_distinct):
                    target_(target), 
                    names_(std::move(names)), 
                    types_(std::move(types)), 
                    not_null_constraints_(std::move(not_null_constraints)), 
                    uniques_(std::move(uniques)) {
        //insert internal column _rowid
        not_null_constraints_.insert(not_null_constraints_.begin(), true);
        names_.insert(names_.begin(), Token("_rowid", TokenType::Identifier));
        types_.insert(types_.begin(), Token("int8", TokenType::Int8));

        //use _rowid if user doesn't specify primary key
        if (primary_keys.empty())
            primary_keys.push_back(Token("_rowid", TokenType::Identifier));

        for (size_t i = 0; i < uniques_.size(); i++) {
            std::vector<Token>& cols = uniques_.at(i);

            //add _rowid to unique column group with 'nulls distinct' clause to differentiate between null fields
            if (nulls_distinct.at(i))
                cols.push_back(Token("_rowid", TokenType::Identifier));
        }

        //an index is created on all uniques, so primary key is decomposed into the 'unique' + 'not null' constraints
        //automatically apply 'not null' constraint to columns in column group of primary key
        for (size_t i = 0; i < names_.size(); i++) {
            if (TokenIn(names_.at(i), primary_keys))
                not_null_constraints_.at(i) = true;
        }

        //insert primary key column group into uniques
        uniques_.insert(uniques_.begin(), primary_keys);
    }
    StmtType Type() const override {
        return StmtType::Create;
    }
public:
    Token target_;
    std::vector<Token> names_;
    std::vector<Token> types_;
    std::vector<bool> not_null_constraints_;
    std::vector<std::vector<Token>> uniques_;
};

class SelectStmt: public Stmt {
public:
    SelectStmt(WorkTable* target, 
               std::vector<Expr*> projs,
               Expr* where_clause, 
               std::vector<OrderCol> order_cols,
               Expr* limit,
               bool remove_duplicates):
                    target_(target),
                    projs_(std::move(projs)),
                    where_clause_(where_clause),
                    order_cols_(std::move(order_cols)),
                    limit_(limit),
                    remove_duplicates_(remove_duplicates),
                    row_description_({}) {}

    StmtType Type() const override {
        return StmtType::Select;
    }
public:
    WorkTable* target_;
    std::vector<Expr*> projs_;
    Expr* where_clause_;
    std::vector<OrderCol> order_cols_;
    Expr* limit_;
    bool remove_duplicates_;
    std::vector<Attribute> row_description_;
};

class InsertStmt: public Stmt {
public:
    InsertStmt(Token target, std::vector<Token> attrs, std::vector<std::vector<Expr*>> values):
        target_(target), attrs_(std::move(attrs)), values_(std::move(values)), col_assigns_({}) {}
    StmtType Type() const override {
        return StmtType::Insert;
    }
public:
    Token target_;
    Table* table_;
    std::vector<Token> attrs_;
    std::vector<std::vector<Expr*>> values_;
    std::vector<std::vector<Expr*>> col_assigns_;
};

class UpdateStmt: public Stmt {
public:
    UpdateStmt(Token target, std::vector<Expr*> assigns, Expr* where_clause):
        target_(target), assigns_(std::move(assigns)), where_clause_(where_clause) {}
    StmtType Type() const override {
        return StmtType::Update;
    }
public:
    Token target_;
    Table* table_;
    std::vector<Expr*> assigns_;
    Expr* where_clause_;
};

class DeleteStmt: public Stmt {
public:
    DeleteStmt(Token target, Expr* where_clause): 
        target_(target), where_clause_(where_clause) {}
    StmtType Type() const override {
        return StmtType::Delete;
    }
public:
    Token target_;
    Table* table_;
    Expr* where_clause_;
};

class DropTableStmt: public Stmt {
public:
    DropTableStmt(Token target_relation, bool has_if_exists):
        target_relation_(target_relation), has_if_exists_(has_if_exists), table_(nullptr) {}
    StmtType Type() const override {
        return StmtType::DropTable;
    }
public:
    Token target_relation_;
    bool has_if_exists_;
    Table* table_;
};

class DescribeTableStmt: public Stmt {
public:
    DescribeTableStmt(Token target_relation): target_relation_(target_relation), table_(nullptr) {}
    StmtType Type() const override {
        return StmtType::DescribeTable;
    }
public:
    Token target_relation_;
    Table* table_;
};

}

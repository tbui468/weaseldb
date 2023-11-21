#pragma once

#include "expr.h"
#include "token.h"
#include "schema.h"
#include "status.h"

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
    DropTable,
    TxnControl,
    CreateModel,
    DropModel
};

//Putting class Stmt here since we need it in Expr,
//but putting it in stmt.h would cause a circular dependency
class Stmt {
public:
    virtual StmtType Type() const = 0;
};

struct Block {
    Block(): stmts({}), commit_on_success(true) {}
    Block(std::vector<Stmt*> stmts, bool commit_on_success): stmts(std::move(stmts)), commit_on_success(commit_on_success) {}
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
    SelectStmt(Scan* scan): scan_(scan) {}
    StmtType Type() const override {
        return StmtType::Select;
    }
public:
    Scan* scan_; //TODO: this should be a ProjectScan since Select Stmts will only ever have that type of scan
};

class InsertStmt: public Stmt {
public:
    InsertStmt(Scan* scan, std::vector<Token> attrs, std::vector<std::vector<Expr*>> values):
        scan_(scan), attrs_(std::move(attrs)), values_(std::move(values)), col_assigns_({}) {}
    StmtType Type() const override {
        return StmtType::Insert;
    }
public:
    Scan* scan_;
    std::vector<Token> attrs_;
    std::vector<std::vector<Expr*>> values_;
    std::vector<std::vector<Expr*>> col_assigns_;
};

class UpdateStmt: public Stmt {
public:
    UpdateStmt(std::vector<Expr*> assigns, Scan* scan): assigns_(std::move(assigns)), scan_(scan) {}
    StmtType Type() const override {
        return StmtType::Update;
    }
public:
    std::vector<Expr*> assigns_;
    Scan* scan_;
};

class DeleteStmt: public Stmt {
public:
    DeleteStmt(Scan* scan): scan_(scan) {}
    StmtType Type() const override {
        return StmtType::Delete;
    }
public:
    Scan* scan_;
};

class DropTableStmt: public Stmt {
public:
    DropTableStmt(Token target_relation, bool has_if_exists):
        target_relation_(target_relation), has_if_exists_(has_if_exists), schema_(nullptr) {}
    StmtType Type() const override {
        return StmtType::DropTable;
    }
public:
    Token target_relation_;
    bool has_if_exists_;
    Schema* schema_;
};

class DescribeTableStmt: public Stmt {
public:
    DescribeTableStmt(Token target_relation): target_relation_(target_relation), schema_(nullptr) {}
    StmtType Type() const override {
        return StmtType::DescribeTable;
    }
public:
    Token target_relation_;
    Schema* schema_;
};

class TxnControlStmt: public Stmt {
public:
    TxnControlStmt(Token t): t_(t) {}
    StmtType Type() const override {
        return StmtType::TxnControl;
    }
public:
    Token t_;
};

class CreateModelStmt: public Stmt {
public:
    CreateModelStmt(Token name, Token path): name_(name), path_(path) {}
    StmtType Type() const override {
        return StmtType::CreateModel;
    }
public:
    Token name_;
    Token path_;
};

class DropModelStmt: public Stmt {
public:
    DropModelStmt(Token name, bool has_if_exists): name_(name), has_if_exists_(has_if_exists) {}
    StmtType Type() const override {
        return StmtType::DropModel;
    }
public:
    Token name_;
    bool has_if_exists_;
};

}

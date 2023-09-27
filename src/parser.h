#pragma once

#include "token.h"
#include "expr.h"
#include "stmt.h"

namespace wsldb {

class Parser {
public:
    Parser(std::vector<Token> tokens): tokens_(std::move(tokens)), idx_(0) {}

    std::vector<Stmt*> ParseStmts();
private:
    Expr* ParsePrimary();
    Expr* ParseUnary();
    Expr* ParseMultiplicative();
    Expr* ParseAdditive();
    Expr* ParseRelational();
    Expr* ParseEquality();
    Expr* ParseAnd();
    Expr* ParseOr();
    Expr* ParseExpr();
    WorkTable* ParsePrimaryWorkTable();
    WorkTable* ParseBinaryWorkTable();
    WorkTable* ParseWorkTable();
    std::vector<Expr*> ParseTuple();

    inline Token PeekToken() {
        return tokens_.at(idx_);
    }

    inline Token NextToken() {
        return tokens_.at(idx_++);
    }

    Stmt* ParseStmt();

public:
    static std::vector<Txn> GroupIntoTxns(const std::vector<Stmt*>& stmts) {
        std::vector<Txn> txns;
        for (Stmt* stmt: stmts) {
            txns.push_back(Txn({stmt})); //TODO: use 'begin/commit/rollback to split stmts into transations.  Just making each stmt txn for now
        }
        return txns;
    }
private:
    std::vector<Token> tokens_;
    int idx_;
};

}

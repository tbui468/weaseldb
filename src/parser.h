#pragma once

#include "token.h"
#include "expr.h"
#include "stmt.h"

namespace wsldb {

class Parser {
public:
    Parser(std::vector<Token> tokens): tokens_(std::move(tokens)), idx_(0), query_state_(nullptr) {}

    std::vector<Stmt*> ParseStmts(DB* db);
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
private:
    std::vector<Token> tokens_;
    int idx_;
    QueryState* query_state_;
};

}

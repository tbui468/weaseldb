#pragma once

#include "token.h"
#include "expr.h"
#include "stmt.h"
#include "status.h"

namespace wsldb {

class Parser {
public:
    Parser(std::vector<Token> tokens): tokens_(std::move(tokens)), idx_(0) {}

    Status ParseTxns(std::vector<Txn>& txns);
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

    inline Token PeekToken() {
        return tokens_.at(idx_);
    }

    inline Token NextToken() {
        return tokens_.at(idx_++);
    }

    inline bool AdvanceIf(TokenType type) {
        if (PeekToken().type == type) {
            NextToken();
            return true;
        }
        return false;
    }

    Status ParseStmt(Stmt** stmt);
    Status ParseTxn(Txn* tx);
private:
    std::vector<Token> tokens_;
    int idx_;
};

}

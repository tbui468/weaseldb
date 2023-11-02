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
    Status Primary(Expr**);
    Status ParseUnary(Expr**); //Expression is named Unary already
    Status Multiplicative(Expr**);
    Status Additive(Expr**);
    Status Relational(Expr**);
    Status Equality(Expr**);
    Status And(Expr**);
    Status Or(Expr**);
    Status Base(Expr**);

    Status ParsePrimaryWorkTable(WorkTable** wt);
    Status ParseBinaryWorkTable(WorkTable** wt);
    Status ParseWorkTable(WorkTable** wt);

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

#pragma once

#include <string>
#include <iostream>
#include "token.h"
#include "datum.h"

namespace wsldb {


class Expr {
public:
    virtual Datum Eval() = 0;
    virtual std::string ToString() = 0;
};

class Literal: public Expr {
public:
    Literal(Token t): t_(t) {}
    Datum Eval() override {
        return Datum(t_);
    }
    virtual std::string ToString() {
        return t_.lexeme;
    }
private:
    Token t_;
};

class Binary: public Expr {
public:
    Binary(Token op, Expr* left, Expr* right):
        op_(op), left_(left), right_(right) {}

    Datum Eval() override {
        //TODO: evaluate binary expression
    }
    virtual std::string ToString() {
        return "(" + op_.lexeme + " " + left_->ToString() + " " + right_->ToString() + ")";
    }
private:
    Expr* left_;
    Expr* right_;
    Token op_;
};

}

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


}

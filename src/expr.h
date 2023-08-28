#pragma once

#include <string>

namespace wsldb {

class Expr {
public:
    virtual void Eval() = 0;
    virtual std::string ToString() = 0;
};

class IntLiteral: public Expr {
public:
    IntLiteral(Token t): t_(t) {}
    void Eval() override {
    }
    virtual std::string ToString() {
        return t_.lexeme;
    }
private:
    Token t_;
};

class StringLiteral: public Expr {
public:
    StringLiteral(Token t): t_(t) {}
    void Eval() override {
    }
    virtual std::string ToString() {
        return t_.lexeme;
    }
private:
    Token t_;
};

}

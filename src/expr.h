#pragma once

#include <string>
#include <iostream>
#include "token.h"

namespace wsldb {

class Datum {
public:
    Datum(Token t) {
        type_ = t.type;
        switch (t.type) {
            case TokenType::IntLiteral: {
                int value = std::stoi(t.lexeme);
                data_.append((char*)&value, sizeof(int));
                break;
            }
            case TokenType::StringLiteral: {
                int size = t.lexeme.size();
                data_.append((char*)&size, sizeof(int));
                data_ += t.lexeme;
                break;
            }
            default:
                std::cout << "invalid token type for initializing datum" << std::endl;
                break;
        }
    }

    //Datum(const std::string& buf, TokenType type); //deserialize data

    TokenType Type() {
        return type_;
    }

    std::string ToString() {
        return data_;
    }

    std::string AsString() {
        int off = sizeof(int); //string length
        return data_.substr(off, data_.size() - off);
    }

    int AsInt() {
        return *((int*)(data_.data()));
    }
private:
    TokenType type_;
    std::string data_;
};

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

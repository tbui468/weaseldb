#pragma once

#include <string>
#include <iostream>
#include "token.h"
#include "datum.h"
#include "tuple.h"
#include "schema.h"

namespace wsldb {


class Expr {
public:
    virtual Datum Eval(Tuple* tuple) = 0;
    virtual std::string ToString() = 0;
    virtual void Analyze(Schema* schema) = 0;
};

class Literal: public Expr {
public:
    Literal(Token t): t_(t) {}
    Datum Eval(Tuple* tuple) override {
        return Datum(t_);
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    void Analyze(Schema* schema) {
    }
private:
    Token t_;
};

class Binary: public Expr {
public:
    Binary(Token op, Expr* left, Expr* right):
        op_(op), left_(left), right_(right) {}

    Datum Eval(Tuple* tuple) override {
        Datum leftd = left_->Eval(tuple);
        Datum rightd = right_->Eval(tuple);
        switch (op_.type) {
            case TokenType::Equal:
                return Datum(leftd.Compare(rightd).AsInt() == 0);
            case TokenType::NotEqual:
                return Datum(leftd.Compare(rightd).AsInt() != 0);
            case TokenType::Less:
                return Datum(leftd.Compare(rightd).AsInt() < 0);
            case TokenType::LessEqual:
                return Datum(leftd.Compare(rightd).AsInt() <= 0);
            case TokenType::Greater:
                return Datum(leftd.Compare(rightd).AsInt() > 0);
            case TokenType::GreaterEqual:
                return Datum(leftd.Compare(rightd).AsInt() >= 0);
            default:
                std::cout << "invalid op\n";
                return Datum(false); //keep compiler quiet
        }
    }
    std::string ToString() override {
        return "(" + op_.lexeme + " " + left_->ToString() + " " + right_->ToString() + ")";
    }
    void Analyze(Schema* schema) {
        left_->Analyze(schema);
        right_->Analyze(schema);
    }
private:
    Expr* left_;
    Expr* right_;
    Token op_;
};

class ColRef: public Expr {
public:
    ColRef(Token t): t_(t), idx_(-1) {}
    Datum Eval(Tuple* tuple) override {
        return tuple->data.at(idx_);
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    void Analyze(Schema* schema) {
        idx_ = schema->GetFieldIdx(t_.lexeme);
    }
private:
    Token t_;
    int idx_;
};

}

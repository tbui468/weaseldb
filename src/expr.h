#pragma once

#include <string>
#include <iostream>
#include "token.h"
#include "datum.h"
#include "tuple.h"
#include "schema.h"
#include "status.h"

namespace wsldb {


class Expr {
public:
    virtual Datum Eval(Tuple* tuple) = 0;
    virtual std::string ToString() = 0;
    virtual Status Analyze(Schema* schema, TokenType* evaluated_type) = 0;
};

class Literal: public Expr {
public:
    Literal(Token t): t_(t) {}
    Literal(bool b): t_(b ? Token("true", TokenType::TrueLiteral) : Token("false", TokenType::FalseLiteral)) {}
    Datum Eval(Tuple* tuple) override {
        return Datum(t_);
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    Status Analyze(Schema* schema, TokenType* evaluated_type) {
        *evaluated_type = Datum(t_).Type();
        return Status(true, "ok");
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
    Status Analyze(Schema* schema, TokenType* evaluated_type) {
        TokenType left_type;
        TokenType right_type;
        left_->Analyze(schema, &left_type);
        right_->Analyze(schema, &right_type);
        
        switch (op_.type) {
            case TokenType::Equal:
            case TokenType::NotEqual:
            case TokenType::Less:
            case TokenType::LessEqual:
            case TokenType::Greater:
            case TokenType::GreaterEqual:
                if (left_type != right_type) {
                    return Status(false, "Error: Equality and relational operands must be same data types");
                }
                *evaluated_type = TokenType::Bool;
                break;
            default:
                break;
        }

        return Status(true, "ok");
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
    Status Analyze(Schema* schema, TokenType* evaluated_type) {
        idx_ = schema->GetFieldIdx(t_.lexeme);
        if (idx_ == -1) {
            return Status(false, "Error: Column '" + t_.lexeme + "' does not exist");
        }
        *evaluated_type = schema->Type(idx_);
        return Status(true, "ok");
    }
private:
    Token t_;
    int idx_;
};

}

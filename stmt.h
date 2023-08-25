#pragma once

#include "tuple.h"
#include "expr.h"
#include "token.h"

#include <iostream>

namespace wsldb {

class Stmt {
public:
    virtual void Execute() = 0;
    virtual void DebugPrint() = 0;
};

class CreateStmt: public Stmt {
public:
    CreateStmt(Token target, std::vector<Token> attrs, std::vector<Token> types):
        target_(target), attrs_(std::move(attrs)), types_(std::move(types)) {}

    void Execute() override {
        //
    }
    void DebugPrint() override {
        std::cout << "Create:\n";
        std::cout << "\ttable name: " << target_.lexeme << std::endl;
        int i = 0;
        for (const Token& t: attrs_) {
            std::cout << "\t" << t.lexeme << ": " << types_.at(i).lexeme << std::endl;
            i++;
        }
    }
private:
    Token target_;
    std::vector<Token> attrs_;
    std::vector<Token> types_;
};

class InsertStmt: public Stmt {
public:
    InsertStmt(Token target, std::vector<Tuple*> values):
        target_(target), values_(std::move(values)) {}

    void Execute() override {
        //
    }
    void DebugPrint() override {
        std::cout << "Insert:\n";
        std::cout << "\ttarget: " << target_.lexeme << std::endl;
        for (Tuple* tuple: values_) {
            std::cout << "\t";
            for (Expr* e: tuple->exprs) {
                std::cout << e->ToString() << ", ";
            }
            std::cout << "\n";
        }
    }
private:
    Token target_;
    std::vector<Tuple*> values_; //Tuples are lists of expressions
};

class SelectStmt: public Stmt {
public:
    //must implement execute
private:
};

}

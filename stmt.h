#pragma once

#include <iostream>

#include "tuple.h"
#include "expr.h"
#include "token.h"
#include "db.h"
#include "rocksdb/db.h"

namespace wsldb {

class Stmt {
public:
    virtual void Analyze(DB* db) = 0;
    virtual void Execute(DB* db) = 0;
    virtual void DebugPrint() = 0;
};

class CreateStmt: public Stmt {
public:
    CreateStmt(Token target, std::vector<Token> attrs, std::vector<Token> types):
        target_(target), attrs_(std::move(attrs)), types_(std::move(types)) {}

    void Analyze(DB* db) override {

    }

    void Execute(DB* db) override {
        //TODO: batch write system catalogue and new table - should be created atomically

        db->Catalogue()->Put(rocksdb::WriteOptions(), target_.lexeme, SerializeSchema());

        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* rel_handle;
        rocksdb::Status status = rocksdb::DB::Open(options, db->GetPath() + "/" + target_.lexeme, &rel_handle);
        db->AppendRelationHandle(rel_handle);
        //add this table to current catalog (name, schema, etc)
        //create new rocksdb inside current database path, eg /tmp/testdb/planets
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
    //Schema format: int count, (TokenType type, data), (), ...
    std::string SerializeSchema() {
        //Could reserve space in string beforehand to prevent allocations when appending
        std::string buf;

        int count = attrs_.size();
        buf.append((char*)&count, sizeof(count));

        for (int i = 0; i < attrs_.size(); i++) {
            buf.append((char*)&types_.at(i).type, sizeof(TokenType));
            int str_size = attrs_.at(i).lexeme.length();
            buf.append((char*)&str_size, sizeof(int));
            buf += attrs_.at(i).lexeme;
        }

        return buf;
    }

    Token target_;
    std::vector<Token> attrs_;
    std::vector<Token> types_;
};

class InsertStmt: public Stmt {
public:
    InsertStmt(Token target, std::vector<Tuple*> values):
        target_(target), values_(std::move(values)) {}

    void Analyze(DB* db) override {

    }

    void Execute(DB* db) override {
        //open table with given name
        //insert key value pair using Put
        //  key is just first value for now (need to take primary key into account later)
        //  value is all values concatenated together (with each string prefixed with 4 bytes representing the length)
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

#pragma once

#include <iostream>

#include "expr.h"
#include "token.h"
#include "db.h"
#include "schema.h"
#include "rocksdb/db.h"

namespace wsldb {

class Stmt {
public:
    virtual void Analyze(DB* db) = 0;
    virtual void Execute(DB* db) = 0;
    virtual std::string ToString() = 0;
};

class CreateStmt: public Stmt {
public:
    CreateStmt(Token target, const std::vector<Token>& attrs, const std::vector<Token>& types): 
        target_(target), schema_(Schema(attrs, types)) {}

    void Analyze(DB* db) override {

    }

    void Execute(DB* db) override {
        //TODO: batch write system catalogue and new table - should be created atomically

        db->Catalogue()->Put(rocksdb::WriteOptions(), target_.lexeme, schema_.Serialize());

        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* rel_handle;
        rocksdb::Status status = rocksdb::DB::Open(options, db->GetTablePath(target_.lexeme), &rel_handle);
        db->AppendTableHandle(rel_handle);
        //add this table to current catalog (name, schema, etc)
        //create new rocksdb inside current database path, eg /tmp/testdb/planets
    }
    std::string ToString() override {
        return "create";
    }
private:
    Token target_;
    Schema schema_;
};

class InsertStmt: public Stmt {
public:
    InsertStmt(Token target, std::vector<std::vector<Expr*>> values):
        target_(target), values_(std::move(values)) {}

    void Analyze(DB* db) override {

    }

    void Execute(DB* db) override {
        std::string value;
        rocksdb::Status status = db->Catalogue()->Get(rocksdb::ReadOptions(), target_.lexeme, &value);
        if (!status.ok()) {
            std::cout << "table " << target_.lexeme <<  " not found" << std::endl;
            return;
        }

        Schema schema(value);

        if (schema.Count() != values_.size()) {
            std::cout << "number of values must match schema" << std::endl;
            return;
        }

        rocksdb::DB* tab_handle = db->GetTableHandle(target_.lexeme);
        for (std::vector<Expr*> tuple: values_) {
            std::string key;
            std::string rec;
            int i = 0;
            for (Expr* e: tuple) {
                Datum d = e->Eval();
                if (schema.PrimaryKeyIdx() == i) {
                    key = d.ToString();
                }
                rec += d.ToString();
                i++;
            }
            tab_handle->Put(rocksdb::WriteOptions(), key, rec);
        }

    }
    std::string ToString() override {
        return "insert";
    }
private:
    Token target_;
    std::vector<std::vector<Expr*>> values_;
};

class SelectStmt: public Stmt {
public:
    //must implement execute
private:
};

}

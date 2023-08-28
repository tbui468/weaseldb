#pragma once

#include <iostream>

#include "expr.h"
#include "token.h"
#include "db.h"
#include "schema.h"
#include "status.h"
#include "rocksdb/db.h"

namespace wsldb {

class Stmt {
public:
    virtual void Analyze(DB* db) = 0;
    virtual Status Execute(DB* db) = 0;
    virtual std::string ToString() = 0;
};

class CreateStmt: public Stmt {
public:
    CreateStmt(Token target, const std::vector<Token>& attrs, const std::vector<Token>& types): 
        target_(target), schema_(Schema(attrs, types)) {}

    void Analyze(DB* db) override {
        //TODO: return error if table with this name already exists
    }

    Status Execute(DB* db) override {
        //TODO: batch write system catalogue and new table - should be created atomically

        db->Catalogue()->Put(rocksdb::WriteOptions(), target_.lexeme, schema_.Serialize());

        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* rel_handle;
        rocksdb::Status status = rocksdb::DB::Open(options, db->GetTablePath(target_.lexeme), &rel_handle);
        db->AppendTableHandle(rel_handle);

        return Status(true, "CREATE TABLE");
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

    Status Execute(DB* db) override {
        std::string value;
        rocksdb::Status status = db->Catalogue()->Get(rocksdb::ReadOptions(), target_.lexeme, &value);
        if (!status.ok()) {
            return Status(false, "Error: invalid table name");
        }

        Schema schema(value);

        if (schema.Count() != values_.size()) {
            return Status(false, "Error: value count does not match schema on record");
        }

        rocksdb::DB* tab_handle = db->GetTableHandle(target_.lexeme);
        for (std::vector<Expr*> exprs: values_) {
            std::vector<Datum> data = std::vector<Datum>();
            for (Expr* e: exprs) {
                data.push_back(e->Eval());
            }
            std::string value = schema.SerializeData(data);
            std::string key = schema.GetKeyFromData(data);
            tab_handle->Put(rocksdb::WriteOptions(), key, value);
        }

        return Status(true, "INSERT " + std::to_string(values_.size()));

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
    SelectStmt(Token target, std::vector<Token> fields): 
        target_(target), fields_(std::move(fields)) {}
    void Analyze(DB* db) override {
    }
    Status Execute(DB* db) override {
        std::vector<std::string> tuplefields = std::vector<std::string>();
        for (const Token& t: fields_) {
            tuplefields.push_back(t.lexeme);
        }

        TupleSet* tupleset = new TupleSet(tuplefields);
        std::string value;
        rocksdb::Status status = db->Catalogue()->Get(rocksdb::ReadOptions(), target_.lexeme, &value);
        Schema schema(value);

        //TODO: iterate through all keys/values of target_
        rocksdb::DB* tab_handle = db->GetTableHandle(target_.lexeme);
        rocksdb::Iterator* it = tab_handle->NewIterator(rocksdb::ReadOptions());

        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string value = it->value().ToString();
            std::vector<Datum> data = schema.DeserializeData(value);
            tupleset->tuples.push_back(new Tuple(data));
        }

        return Status(true, "(" + std::to_string(tupleset->tuples.size()) + " rows)", tupleset);
    }
    std::string ToString() override {
        return "select";
    }
private:
    Token target_;
    std::vector<Token> fields_;
};

}

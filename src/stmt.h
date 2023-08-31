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
    virtual Status Analyze(DB* db) = 0;
    virtual Status Execute(DB* db) = 0;
    virtual std::string ToString() = 0;
};

class CreateStmt: public Stmt {
public:
    CreateStmt(Token target, std::vector<Attribute*> attributes, std::vector<Token> primary_keys):
        target_(target), attributes_(std::move(attributes)), primary_keys_(std::move(primary_keys)) {}

    Status Analyze(DB* db) override {
        std::string serialized_schema;
        if (db->TableSchema(target_.lexeme, &serialized_schema)) {
            return Status(false, "Error: Table '" + target_.lexeme + "' already exists");
        }

        for (Attribute* a: attributes_) {
            if (a->name.type != TokenType::Identifier) {
                return Status(false, "Error: '" + a->name.lexeme + "' is not allowed as column name");
            }
            if (!Datum::ValidType(a->type.type)) {
                return Status(false, "Error: '" + a->type.lexeme + "' is not a valid data type");
            }
        }

        Schema schema(attributes_, primary_keys_);
        for (Token t: primary_keys_) {
            if (schema.GetFieldIdx(t.lexeme) == -1) {
                return Status(false, "Error: Column '" + t.lexeme + "' not declared in table '" + target_.lexeme + "'");
            }
        }

        //TODO: verify that foreign table in foreign keys exists
        //TODO: verify that foreign columns in foreign keys exist

        return Status(true, "ok");
    }

    Status Execute(DB* db) override {
        Schema schema(attributes_, primary_keys_);
        db->Catalogue()->Put(rocksdb::WriteOptions(), target_.lexeme, schema.Serialize());

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
    std::vector<Attribute*> attributes_;
    std::vector<Token> primary_keys_;
};

class InsertStmt: public Stmt {
public:
    InsertStmt(Token target, std::vector<std::vector<Expr*>> values):
        target_(target), values_(std::move(values)) {}

    Status Analyze(DB* db) override {
        std::string serialized_schema;
        if (!db->TableSchema(target_.lexeme, &serialized_schema)) {
            return Status(false, "Error: Table '" + target_.lexeme + "' does not exist");
        }
        Schema schema(serialized_schema);

        for (const std::vector<Expr*>& exprs: values_) {
            if (exprs.size() != schema.FieldCount()) {
                return Status(false, "Error: Value count does not match field count in schema on record");
            }
            int i = 0;
            for (Expr* e: exprs) {
                TokenType type;
                Status status = e->Analyze(&schema, &type);

                if (!status.Ok()) {
                    return status;
                }

                if (type != schema.Type(i)) {
                    return Status(false, "Error: Value type does not match type in schema on record");
                }

                i++;
            }
        }

        return Status(true, "ok");
    }

    Status Execute(DB* db) override {
        std::string serialized_schema;
        db->TableSchema(target_.lexeme, &serialized_schema);
        Schema schema(serialized_schema);

        rocksdb::DB* tab_handle = db->GetTableHandle(target_.lexeme);
        for (std::vector<Expr*> exprs: values_) {
            std::vector<Datum> data = std::vector<Datum>();
            for (Expr* e: exprs) {
                data.push_back(e->Eval(nullptr));
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
    SelectStmt(Token target_relation, std::vector<Expr*> target_cols, Expr* where_clause): 
        target_relation_(target_relation), target_cols_(std::move(target_cols)), where_clause_(where_clause) {}

    Status Analyze(DB* db) override {
        std::string serialized_schema;
        if (!db->TableSchema(target_relation_.lexeme, &serialized_schema)) {
            return Status(false, "Error: Table '" + target_relation_.lexeme + "' does not exist");
        }
        Schema schema(serialized_schema);

        TokenType type;
        Status status = where_clause_->Analyze(&schema, &type);
        if (!status.Ok()) {
            return status;
        }

        if (type != TokenType::Bool) {
            return Status(false, "Error: Where clause must be a boolean expression");
        }

        for (Expr* e: target_cols_) {
            TokenType type;
            Status status = e->Analyze(&schema, &type);
            if (!status.Ok()) {
                return status;
            }
        }

        return Status(true, "ok");
    }
    Status Execute(DB* db) override {
        std::vector<std::string> tuplefields = std::vector<std::string>();
        for (Expr* e: target_cols_) {
            tuplefields.push_back(e->ToString()); //TODO: this won't work if e is not a ColRef
        }

        TupleSet* tupleset = new TupleSet(tuplefields);

        std::string serialized_schema;
        rocksdb::Status status = db->Catalogue()->Get(rocksdb::ReadOptions(), target_relation_.lexeme, &serialized_schema);
        Schema schema(serialized_schema);

        rocksdb::DB* tab_handle = db->GetTableHandle(target_relation_.lexeme);
        rocksdb::Iterator* it = tab_handle->NewIterator(rocksdb::ReadOptions());

        //index scan
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string value = it->value().ToString();

            //selection
            Tuple tuple(schema.DeserializeData(value));
            if (!where_clause_->Eval(&tuple).AsBool())
                continue;

            //projection
            std::vector<Datum> final_tuple = std::vector<Datum>();

            for (Expr* e: target_cols_) {
                final_tuple.push_back(e->Eval(&tuple));
            }

            //add final tuple to output set
            tupleset->tuples.push_back(new Tuple(final_tuple));
        }

        return Status(true, "(" + std::to_string(tupleset->tuples.size()) + " rows)", tupleset);
    }
    std::string ToString() override {
        return "select";
    }
private:
    Token target_relation_; //TODO: should be expression too to allow subqueries
    std::vector<Expr*> target_cols_;
    Expr* where_clause_;
};

class UpdateStmt: public Stmt {
public:
    UpdateStmt(Token target_relation, std::vector<Expr*> assignments, Expr* where_clause):
        target_relation_(target_relation), assignments_(std::move(assignments)), where_clause_(where_clause) {}
    Status Analyze(DB* db) override {
        std::string serialized_schema;
        if (!db->TableSchema(target_relation_.lexeme, &serialized_schema)) {
            return Status(false, "Error: Table '" + target_relation_.lexeme + "' does not exist");
        }
        Schema schema(serialized_schema);

        TokenType type;
        Status status = where_clause_->Analyze(&schema, &type);
        if (!status.Ok()) {
            return status;
        }

        for (Expr* e: assignments_) {
            TokenType type;
            Status status = e->Analyze(&schema, &type);
            if (!status.Ok()) {
                return status;
            }
        }
        return Status(true, "ok");
    }
    Status Execute(DB* db) override {
        std::string serialized_schema;
        rocksdb::Status status = db->Catalogue()->Get(rocksdb::ReadOptions(), target_relation_.lexeme, &serialized_schema);
        Schema schema(serialized_schema);

        rocksdb::DB* tab_handle = db->GetTableHandle(target_relation_.lexeme);
        rocksdb::Iterator* it = tab_handle->NewIterator(rocksdb::ReadOptions());

        //index scan
        int update_count = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string value = it->value().ToString();

            //selection
            Tuple tuple(schema.DeserializeData(value));
            if (!where_clause_->Eval(&tuple).AsBool())
                continue;

            update_count++;

            for (Expr* e: assignments_) {
                e->Eval(&tuple);  //return Datum is not used
            }

            std::string updated_value = schema.SerializeData(tuple.data);
            std::string updated_key = schema.GetKeyFromData(tuple.data);
            tab_handle->Put(rocksdb::WriteOptions(), updated_key, updated_value);
        }
        return Status(true, "(" + std::to_string(update_count) + " updates)");
    }
    std::string ToString() override {
        return "update";
    }
private:
    Token target_relation_;
    std::vector<Expr*> assignments_;
    Expr* where_clause_;
};

class DeleteStmt: public Stmt {
public:
    DeleteStmt(Token target_relation, Expr* where_clause): 
        target_relation_(target_relation), where_clause_(where_clause) {}
    Status Analyze(DB* db) override {
        std::string serialized_schema;
        if (!db->TableSchema(target_relation_.lexeme, &serialized_schema)) {
            return Status(false, "Error: Table '" + target_relation_.lexeme + "' does not exist");
        }
        Schema schema(serialized_schema);

        TokenType type;
        Status status = where_clause_->Analyze(&schema, &type);
        if (!status.Ok()) {
            return status;
        }
        return Status(true, "ok");
    }
    Status Execute(DB* db) override {
        std::string serialized_schema;
        rocksdb::Status status = db->Catalogue()->Get(rocksdb::ReadOptions(), target_relation_.lexeme, &serialized_schema);
        Schema schema(serialized_schema);

        rocksdb::DB* tab_handle = db->GetTableHandle(target_relation_.lexeme);
        rocksdb::Iterator* it = tab_handle->NewIterator(rocksdb::ReadOptions());

        //index scan
        int delete_count = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string value = it->value().ToString();

            //selection
            Tuple tuple(schema.DeserializeData(value));
            if (!where_clause_->Eval(&tuple).AsBool())
                continue;

            delete_count++;

            tab_handle->Delete(rocksdb::WriteOptions(), it->key().ToString());
        }
        return Status(true, "(" + std::to_string(delete_count) + " deletes)");
    }
    std::string ToString() override {
        return "delete";
    }
private:
    Token target_relation_;
    Expr* where_clause_;
};

class DropTableStmt: public Stmt {
public:
    DropTableStmt(Token target_relation, bool has_if_exists):
        target_relation_(target_relation), has_if_exists_(has_if_exists) {}
    Status Analyze(DB* db) override {
        if (!has_if_exists_) {
            std::string serialized_schema;
            if (!db->TableSchema(target_relation_.lexeme, &serialized_schema)) {
                return Status(false, "Error: Table '" + target_relation_.lexeme + "' does not exist");
            }
        }
        return Status(true, "ok");
    }
    Status Execute(DB* db) override {
        db->Catalogue()->Delete(rocksdb::WriteOptions(), target_relation_.lexeme);
        bool dropped = db->DropTable(target_relation_.lexeme);
        if (dropped) {
            return Status(true, "(table '" + target_relation_.lexeme + "' dropped)");
        } else {
            return Status(true, "(table '" + target_relation_.lexeme + "' doesn't exist and not dropped)");
        }
    }
    std::string ToString() override {
        return "drop table";
    }
private:
    Token target_relation_;
    bool has_if_exists_;
};

class DescribeTableStmt: public Stmt {
public:
    DescribeTableStmt(Token target_relation): target_relation_(target_relation) {}
    Status Analyze(DB* db) override {
        std::string serialized_schema;
        if (!db->TableSchema(target_relation_.lexeme, &serialized_schema)) {
            return Status(false, "Error: Table '" + target_relation_.lexeme + "' does not exist");
        }
        return Status(true, "ok");
    }
    Status Execute(DB* db) override {
        std::string serialized_schema;
        rocksdb::Status status = db->Catalogue()->Get(rocksdb::ReadOptions(), target_relation_.lexeme, &serialized_schema);
        Schema schema(serialized_schema);

        std::vector<std::string> tuplefields = std::vector<std::string>();
        tuplefields.push_back("Column");
        tuplefields.push_back("Type");
        TupleSet* tupleset = new TupleSet(tuplefields);

        for (int i = 0; i < schema.FieldCount(); i++) {
            std::vector<Datum> data = {Datum(schema.Name(i)), Datum(TokenTypeToString(schema.Type(i)))};
            tupleset->tuples.push_back(new Tuple(data));
        }

        std::vector<Datum> pk_data;
        std::string s = "Primary keys:";
        pk_data.push_back(Datum(s));
        for (int i = 0; i < schema.PrimaryKeyCount(); i++) {
            pk_data.push_back(schema.Name(schema.PrimaryKey(i)));
        }
        tupleset->tuples.push_back(new Tuple(pk_data));

        return Status(true, "table '" + target_relation_.lexeme + "'", tupleset);
    }
    std::string ToString() override {
        return "describe table";
    }

private:
    Token target_relation_;
};

}

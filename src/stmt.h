#pragma once

#include <iostream>
#include <unordered_map>

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
    CreateStmt(Token target, std::vector<Token> names, std::vector<Token> types, std::vector<Token> primary_keys):
        target_(target), names_(std::move(names)), types_(std::move(types)), primary_keys_(std::move(primary_keys)) {}

    Status Analyze(DB* db) override {
        std::string serialized_schema;
        if (db->TableSchema(target_.lexeme, &serialized_schema)) {
            return Status(false, "Error: Table '" + target_.lexeme + "' already exists");
        }

        for (int i = 0; i < names_.size(); i++) {
            Token name = names_.at(i);
            Token type = types_.at(i);
            if (name.type != TokenType::Identifier) {
                return Status(false, "Error: '" + name.lexeme + "' is not allowed as column name");
            }
            if (!TokenTypeValidDataType(type.type)) {
                return Status(false, "Error: '" + type.lexeme + "' is not a valid data type");
            }
        }

        Schema schema(target_.lexeme, names_, types_, primary_keys_);
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
        Schema schema(target_.lexeme, names_, types_, primary_keys_);
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
    std::vector<Token> names_;
    std::vector<Token> types_;
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
        Schema schema(target_.lexeme, serialized_schema);

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
        Schema schema(target_.lexeme, serialized_schema);

        rocksdb::DB* tab_handle = db->GetTableHandle(target_.lexeme);
        Row dummy_row({});
        for (std::vector<Expr*> exprs: values_) {
            std::vector<Datum> data = std::vector<Datum>();
            Datum d;
            for (Expr* e: exprs) {
                Status s = e->Eval(&dummy_row, &d);
                if (!s.Ok()) return s;

                data.push_back(d);
            }
            std::string value = schema.SerializeData(data);
            std::string key = schema.GetKeyFromData(data);

            std::string test_value;
            rocksdb::Status status = tab_handle->Get(rocksdb::ReadOptions(), key, &test_value);
            if (status.ok()) {
                return Status(false, "Error: A record with the same primary key already exists");
            }

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
    SelectStmt(std::vector<WorkTableOld> ranges, 
               std::vector<Expr*> projs,
               Expr* where_clause, 
               std::vector<OrderCol> order_cols,
               Expr* limit,
               bool remove_duplicates):
                    ranges_(std::move(ranges)),
                    projs_(std::move(projs)),
                    where_clause_(where_clause),
                    order_cols_(std::move(order_cols)),
                    limit_(limit),
                    remove_duplicates_(remove_duplicates) {}

    Status Analyze(DB* db) override {
        Schema* schema = nullptr;
        if (!ranges_.empty()) {
            std::string serialized_schema;
            Row dummy_row({});
            Datum d;
            for (WorkTableOld wt: ranges_) {
                Expr* e = wt.table;
                Status s = e->Eval(&dummy_row, &d);
                if (!s.Ok()) return s;

                std::string target_relation = d.AsString();
                if (!db->TableSchema(target_relation, &serialized_schema)) {
                    return Status(false, "Error: Table '" + target_relation + "' does not exist");
                }
                schema = new Schema(wt.alias.lexeme, serialized_schema); //alias is same as table name if not specified
            }
        } else {
            schema = new Schema("", {}, {}, {});
        }

        //projection
        for (Expr* e: projs_) {
            TokenType type;
            Status status = e->Analyze(schema, &type);
            if (!status.Ok()) {
                return status;
            }
        }

        //where clause
        {
            TokenType type;
            Status status = where_clause_->Analyze(schema, &type);
            if (!status.Ok()) {
                return status;
            }

            if (type != TokenType::Bool) {
                return Status(false, "Error: Where clause must be a boolean expression");
            }
        }

        //order cols
        for (OrderCol oc: order_cols_) {
            TokenType type;
            Status status = oc.col->Analyze(schema, &type);
            if (!status.Ok())
                return status;

            status = oc.asc->Analyze(schema, &type);
            if (!status.Ok()) {
                return status;
            }
        }

        //limit
        {
            TokenType type;
            Status status = limit_->Analyze(schema, &type);
            if (!status.Ok()) {
                return status;     
            }

            if (type != TokenType::Int4) {
                return Status(false, "Error: 'Limit' must be followed by an expression that evaluates to an integer");
            }
        }

        return Status(true, "ok");
    }
    Status Execute(DB* db) override {
        RowSet* rs = new RowSet();
        int offset = 0;
        for (WorkTableOld wt: ranges_) {
            Row dummy_row({});
            std::string serialized_schema;
            Datum d;
            Status s = wt.table->Eval(&dummy_row, &d);
            if (!s.Ok()) return s;
            std::string target_relation = d.AsString();
            db->TableSchema(target_relation, &serialized_schema);
            Schema schema(wt.alias.lexeme, serialized_schema);

            rocksdb::DB* tab_handle = db->GetTableHandle(target_relation);
            rocksdb::Iterator* it = tab_handle->NewIterator(rocksdb::ReadOptions());

            //index scan
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                std::string value = it->value().ToString();
                rs->rows_.push_back(new Row(schema.DeserializeData(value)));
            }

            std::vector<Attribute>* attrs = new std::vector<Attribute>();
            for (int i = 0; i < schema.FieldCount(); i++) {
                attrs->emplace_back(Attribute(schema.Name(i), schema.Type(i), offset));
                offset++;
            }

            //put both table name and alias (which may be same as table name) since SQL can reference either
            //if there is no ambiguity.  Need to report error if there is ambiguity.
            rs->attrs_.insert({ target_relation, attrs });
            rs->attrs_.insert({ wt.alias.lexeme, attrs });
        }

        //if no input tables are provided
        if (ranges_.empty()) {
            rs->rows_.push_back(new Row({}));
        }

        //apply where clause here to filter rs
        Expr* where = where_clause_;
        rs->rows_.erase(std::remove_if(rs->rows_.begin(), rs->rows_.end(),
                    [where](Row* r) -> bool {
                        Datum d;
                        where->Eval(r, &d);
                        return !d.AsBool();
                    }), rs->rows_.end());

        //sort filtered rows in-place
        if (!order_cols_.empty()) {
            std::vector<OrderCol>& order_cols = order_cols_; //lambdas can only capture non-member variable

            std::sort(rs->rows_.begin(), rs->rows_.end(), 
                        //No error checking in lambda...
                        [order_cols](Row* t1, Row* t2) -> bool { 
                            for (OrderCol oc: order_cols) {
                                Datum d1;
                                oc.col->Eval(t1, &d1);
                                Datum d2;
                                oc.col->Eval(t2, &d2);
                                if (d1 == d2)
                                    continue;

                                Row* r = nullptr;
                                Datum d;
                                oc.asc->Eval(r, &d);
                                if (d.AsBool()) {
                                    return d1 < d2;
                                }
                                return d1 > d2;
                            }

                            return true;
                        });
        }

        //projection
        RowSet* proj_rs = new RowSet();
        for (Row* r: rs->rows_) {
            proj_rs->rows_.push_back(new Row({}));
        }

        for (Expr* e: projs_) {
            std::vector<Datum> col;
            Status s = e->Eval(rs, col);
            if (!s.Ok()) {
                return s;
            }
            if (col.size() < proj_rs->rows_.size()) {
                proj_rs->rows_.resize(col.size());
            } else if (col.size() > proj_rs->rows_.size()) {
                return Status(false, "Error: Mixing column references with and without aggregation functions causes mismatched column sizes!");
            }

            for (int i = 0; i < col.size(); i++) {
                proj_rs->rows_.at(i)->data_.push_back(col.at(i));
            }
        }

        //remove duplicates
        RowSet* final_rs = new RowSet();
        if (remove_duplicates_) {
            std::unordered_map<std::string, bool> map;
            for (Row* r: proj_rs->rows_) {
                std::string key = Datum::SerializeData(r->data_);
                if (map.find(key) == map.end()) {
                    map.insert({key, true});
                    final_rs->rows_.push_back(r);
                }
            }
        } else {
            final_rs = proj_rs;
        }

        //limit in-place
        Row dummy_row({});
        Datum d;
        Status s = limit_->Eval(&dummy_row, &d);
        if (!s.Ok()) return s;

        int limit = d.AsInt4();
        if (limit != -1 && limit < final_rs->rows_.size()) {
            final_rs->rows_.resize(limit);
        }

        return Status(true, "(" + std::to_string(final_rs->rows_.size()) + " rows)", final_rs);
    }
    std::string ToString() override {
        return "select";
    }
private:
    std::vector<WorkTableOld> ranges_;
    std::vector<Expr*> projs_;
    Expr* where_clause_;
    std::vector<OrderCol> order_cols_;
    Expr* limit_;
    bool remove_duplicates_;
};

class UpdateStmt: public Stmt {
public:
    UpdateStmt(WorkTable* target, std::vector<Expr*> assigns, Expr* where_clause):
        target_(target), assigns_(std::move(assigns)), where_clause_(where_clause) {}
    Status Analyze(DB* db) override {
        {
            Status s = target_->Analyze(db);
            if (!s.Ok())
                return s;
        }

        {
            TokenType type;
            Status s = where_clause_->Analyze(target_->GetAttributes(), &type);
            if (!s.Ok())
                return s;
            if (type != TokenType::Bool)
                return Status(false, "Error: 'where' clause must evaluated to a boolean value");
        }

        for (Expr* e: assigns_) {
            TokenType type;
            Status s = e->Analyze(target_->GetAttributes(), &type);
            if (!s.Ok())
                return s;
        }
        
        return Status(true, "ok");
    }
    Status Execute(DB* db) override {
        target_->BeginScan(db);

        Row* r;
        int update_count = 0;
        Datum d;
        while (target_->NextRow(db, &r).Ok()) {
            Status s = where_clause_->Eval(r, &d);
            if (!s.Ok()) 
                return s;

            if (!d.AsBool())
                continue;

            for (Expr* e: assigns_) {
                Status s = e->Eval(r, &d); //returned Datum of ColAssign expressions are ignored
                if (!s.Ok()) 
                    return s;
            }
            target_->UpdatePrev(db, r);
            update_count++;
        }

        target_->EndScan(db);

        return Status(true, "(" + std::to_string(update_count) + " deletes)");
    }
    std::string ToString() override {
        return "update";
    }
private:
    WorkTable* target_;
    std::vector<Expr*> assigns_;
    Expr* where_clause_;
};

class DeleteStmt: public Stmt {
public:
    DeleteStmt(WorkTable* target, Expr* where_clause): 
        target_(target), where_clause_(where_clause) {}
    Status Analyze(DB* db) override {
        {
            Status s = target_->Analyze(db);
            if (!s.Ok())
                return s;
        }

        {
            TokenType type;
            Status s = where_clause_->Analyze(target_->GetAttributes(), &type);
            if (!s.Ok())
                return s;
            if (type != TokenType::Bool)
                return Status(false, "Error: 'where' clause must evaluated to a boolean value");
        }

        return Status(true, "ok");
    }
    Status Execute(DB* db) override {
        target_->BeginScan(db);

        Row* r;
        int delete_count = 0;
        Datum d;
        while (target_->NextRow(db, &r).Ok()) {
            Status s = where_clause_->Eval(r, &d);
            if (!s.Ok()) 
                return s;

            if (!d.AsBool())
                continue;

            target_->DeletePrev(db);
            delete_count++;
        }

        target_->EndScan(db);

        return Status(true, "(" + std::to_string(delete_count) + " deletes)");
    }
    std::string ToString() override {
        return "delete";
    }
private:
    WorkTable* target_;
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
        Schema schema(target_relation_.lexeme, serialized_schema);

        std::vector<std::string> tuplefields = std::vector<std::string>();
        tuplefields.push_back("Column");
        tuplefields.push_back("Type");
        RowSet* rowset = new RowSet();

        for (int i = 0; i < schema.FieldCount(); i++) {
            std::vector<Datum> data = {Datum(schema.Name(i)), Datum(TokenTypeToString(schema.Type(i)))};
            rowset->rows_.push_back(new Row(data));
        }

        std::vector<Datum> pk_data;
        std::string s = "Primary keys:";
        pk_data.push_back(Datum(s));
        for (int i = 0; i < schema.PrimaryKeyCount(); i++) {
            pk_data.push_back(schema.Name(schema.PrimaryKey(i)));
        }
        rowset->rows_.push_back(new Row(pk_data));

        return Status(true, "table '" + target_relation_.lexeme + "'", rowset);
    }
    std::string ToString() override {
        return "describe table";
    }

private:
    Token target_relation_;
};

}

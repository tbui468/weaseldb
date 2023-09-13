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

//Moved class Stmt declaration to expr.h since we need it there,
//but including stmt.h would cause a circular dependency

class CreateStmt: public Stmt {
public:
    CreateStmt(QueryState* qs, Token target, std::vector<Token> names, std::vector<Token> types, std::vector<Token> primary_keys):
        Stmt(qs), target_(target), names_(std::move(names)), types_(std::move(types)), primary_keys_(std::move(primary_keys)) {}

    Status Analyze(std::vector<TokenType>& types) override {
        std::string serialized_schema;
        if (GetQueryState()->db->TableSchema(target_.lexeme, &serialized_schema)) {
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

    Status Execute() override {
        Schema schema(target_.lexeme, names_, types_, primary_keys_);
        GetQueryState()->db->Catalogue()->Put(rocksdb::WriteOptions(), target_.lexeme, schema.Serialize());

        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* rel_handle;
        rocksdb::Status status = rocksdb::DB::Open(options, GetQueryState()->db->GetTablePath(target_.lexeme), &rel_handle);
        GetQueryState()->db->AppendTableHandle(rel_handle);

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
    InsertStmt(QueryState* qd, WorkTable* target, std::vector<std::vector<Expr*>> values):
        Stmt(qd), target_(target), values_(std::move(values)) {}

    Status Analyze(std::vector<TokenType>& types) override {
        AttributeSet* working_attrs;
        {
            Status s = target_->Analyze(GetQueryState(), &working_attrs);
            if (!s.Ok())
                return s;
            GetQueryState()->PushAnalysisScope(working_attrs);
        }

        std::vector<std::string> tables = working_attrs->TableNames();
        if (tables.size() != 1)
            return Status(false, "Error: Cannot insert into more than one table");

        std::string table = tables.at(0);
        std::vector<Attribute>* attrs = working_attrs->TableAttributes(table);

        for (const std::vector<Expr*>& exprs: values_) {
            if (exprs.size() != attrs->size()) {
                return Status(false, "Error: Value count does not match attribute count in schema on record");
            }

            for (int i = 0; i < exprs.size(); i++) {
                Expr* e = exprs.at(i);
                TokenType type;
                Status s = e->Analyze(GetQueryState(), &type);
                if (!s.Ok())
                    return s;
                Attribute a = attrs->at(i);
                if (attrs->at(i).type != type)
                    return Status(false, "Error: Value type does not match type in schema on record");
            }
        }

        GetQueryState()->PopAnalysisScope();

        return Status(true, "ok");
    }

    Status Execute() override {
        Row dummy_row({});
        for (std::vector<Expr*> exprs: values_) {
            std::vector<Datum> data = std::vector<Datum>();
            for (Expr* e: exprs) {
                Datum d;
                Status s = e->Eval(&dummy_row, &d);
                if (!s.Ok()) return s;

                data.push_back(d);
            }
            Status s = target_->Insert(GetQueryState()->db, data);
            if (!s.Ok())
                return s;
        }

        return Status(true, "INSERT " + std::to_string(values_.size()));

    }
    std::string ToString() override {
        return "insert";
    }
private:
    WorkTable* target_;
    std::vector<std::vector<Expr*>> values_;
};

class SelectStmt: public Stmt {
public:
    SelectStmt(QueryState* qd, WorkTable* target, 
               std::vector<Expr*> projs,
               Expr* where_clause, 
               std::vector<OrderCol> order_cols,
               Expr* limit,
               bool remove_duplicates):
                    Stmt(qd),
                    target_(target),
                    projs_(std::move(projs)),
                    where_clause_(where_clause),
                    order_cols_(std::move(order_cols)),
                    limit_(limit),
                    remove_duplicates_(remove_duplicates) {}

    Status Analyze(std::vector<TokenType>& types) override {
        AttributeSet* working_attrs;
        {
            Status s = target_->Analyze(GetQueryState(), &working_attrs);
            if (!s.Ok())
                return s;
            GetQueryState()->PushAnalysisScope(working_attrs);
        }

        //projection
        for (Expr* e: projs_) {
            TokenType type;
            Status s = e->Analyze(GetQueryState(), &type);
            if (!s.Ok()) {
                return s;
            }
            types.push_back(type);
        }

        //where clause
        {
            TokenType type;
            Status s = where_clause_->Analyze(GetQueryState(), &type);
            if (!s.Ok()) {
                return s;
            }

            if (type != TokenType::Bool) {
                return Status(false, "Error: Where clause must be a boolean expression");
            }
        }

        //order cols
        for (OrderCol oc: order_cols_) {
            {
                TokenType type;
                Status s = oc.col->Analyze(GetQueryState(), &type);
                if (!s.Ok())
                    return s;
            }

            {
                TokenType type;
                Status s = oc.asc->Analyze(GetQueryState(), &type);
                if (!s.Ok()) {
                    return s;
                }
            }
        }

        //limit
        {
            TokenType type;
            Status s = limit_->Analyze(GetQueryState(), &type);
            if (!s.Ok()) {
                return s;
            }

            if (type != TokenType::Int4) {
                return Status(false, "Error: 'Limit' must be followed by an expression that evaluates to an integer");
            }
        }

        GetQueryState()->PopAnalysisScope();

        return Status(true, "ok");
    }
    Status Execute() override {
        RowSet* rs = new RowSet();
        target_->BeginScan(GetQueryState()->db);

        Row* r;
        while (target_->NextRow(GetQueryState()->db, &r).Ok()) {
            Datum d;
            Status s = where_clause_->Eval(r, &d);
            if (!s.Ok()) 
                return s;

            if (!d.AsBool())
                continue;

            rs->rows_.push_back(r);
        }

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
        std::string result = "select ";
        for (Expr* e: projs_) {
            result += e->ToString() + " ";
        }
        result += "from " + target_->ToString();
        return result;
    }
private:
    WorkTable* target_;
    std::vector<Expr*> projs_;
    Expr* where_clause_;
    std::vector<OrderCol> order_cols_;
    Expr* limit_;
    bool remove_duplicates_;
};

class UpdateStmt: public Stmt {
public:
    UpdateStmt(QueryState* qd, WorkTable* target, std::vector<Expr*> assigns, Expr* where_clause):
        Stmt(qd), target_(target), assigns_(std::move(assigns)), where_clause_(where_clause) {}
    Status Analyze(std::vector<TokenType>& types) override {
        AttributeSet* working_attrs;
        {
            Status s = target_->Analyze(GetQueryState(), &working_attrs);
            if (!s.Ok())
                return s;
            GetQueryState()->PushAnalysisScope(working_attrs);
        }

        {
            TokenType type;
            Status s = where_clause_->Analyze(GetQueryState(), &type);
            if (!s.Ok())
                return s;
            if (type != TokenType::Bool)
                return Status(false, "Error: 'where' clause must evaluated to a boolean value");
        }

        for (Expr* e: assigns_) {
            TokenType type;
            Status s = e->Analyze(GetQueryState(), &type);
            if (!s.Ok())
                return s;
        }

        GetQueryState()->PopAnalysisScope();
        
        return Status(true, "ok");
    }
    Status Execute() override {
        target_->BeginScan(GetQueryState()->db);

        Row* r;
        int update_count = 0;
        Datum d;
        while (target_->NextRow(GetQueryState()->db, &r).Ok()) {
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
            target_->UpdatePrev(GetQueryState()->db, r);
            update_count++;
        }

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
    DeleteStmt(QueryState* qd, WorkTable* target, Expr* where_clause): 
        Stmt(qd), target_(target), where_clause_(where_clause) {}
    Status Analyze(std::vector<TokenType>& types) override {
        AttributeSet* working_attrs;
        {
            Status s = target_->Analyze(GetQueryState(), &working_attrs);
            if (!s.Ok())
                return s;
            GetQueryState()->PushAnalysisScope(working_attrs);
        }

        {
            TokenType type;
            Status s = where_clause_->Analyze(GetQueryState(), &type);
            if (!s.Ok())
                return s;
            if (type != TokenType::Bool)
                return Status(false, "Error: 'where' clause must evaluated to a boolean value");
        }

        GetQueryState()->PopAnalysisScope();

        return Status(true, "ok");
    }
    Status Execute() override {
        target_->BeginScan(GetQueryState()->db);

        Row* r;
        int delete_count = 0;
        Datum d;
        while (target_->NextRow(GetQueryState()->db, &r).Ok()) {
            Status s = where_clause_->Eval(r, &d);
            if (!s.Ok()) 
                return s;

            if (!d.AsBool())
                continue;

            target_->DeletePrev(GetQueryState()->db);
            delete_count++;
        }

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
    DropTableStmt(QueryState* qd, Token target_relation, bool has_if_exists):
        Stmt(qd), target_relation_(target_relation), has_if_exists_(has_if_exists) {}
    Status Analyze(std::vector<TokenType>& types) override {
        if (!has_if_exists_) {
            std::string serialized_schema;
            if (!GetQueryState()->db->TableSchema(target_relation_.lexeme, &serialized_schema)) {
                return Status(false, "Error: Table '" + target_relation_.lexeme + "' does not exist");
            }
        }
        return Status(true, "ok");
    }
    Status Execute() override {
        GetQueryState()->db->Catalogue()->Delete(rocksdb::WriteOptions(), target_relation_.lexeme);
        bool dropped = GetQueryState()->db->DropTable(target_relation_.lexeme);
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
    DescribeTableStmt(QueryState* qd, Token target_relation): Stmt(qd), target_relation_(target_relation) {}
    Status Analyze(std::vector<TokenType>& types) override {
        std::string serialized_schema;
        if (!GetQueryState()->db->TableSchema(target_relation_.lexeme, &serialized_schema)) {
            return Status(false, "Error: Table '" + target_relation_.lexeme + "' does not exist");
        }
        return Status(true, "ok");
    }
    Status Execute() override {
        std::string serialized_schema;
        rocksdb::Status status = GetQueryState()->db->Catalogue()->Get(rocksdb::ReadOptions(), target_relation_.lexeme, &serialized_schema);
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

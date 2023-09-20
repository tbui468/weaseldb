#pragma once

#include <iostream>
#include <unordered_map>
#include <limits>

#include "expr.h"
#include "token.h"
#include "db.h"
#include "table.h"
#include "status.h"
#include "rocksdb/db.h"

namespace wsldb {

//Moved class Stmt declaration to expr.h since we need it there,
//but including stmt.h would cause a circular dependency

class CreateStmt: public Stmt {
public:
    CreateStmt(QueryState* qs, 
               Token target, 
               std::vector<Token> names, 
               std::vector<Token> types, 
               std::vector<bool> not_null_constraints, 
               std::vector<Token> primary_keys):
                    Stmt(qs), 
                    target_(target), 
                    names_(std::move(names)), 
                    types_(std::move(types)), 
                    not_null_constraints_(std::move(not_null_constraints)), 
                    primary_keys_(std::move(primary_keys)) {}

    Status Analyze(std::vector<TokenType>& types) override {
        Table* table_ptr;
        Status s = OpenTable(target_.lexeme, &table_ptr);
        if (s.Ok()) {
            return Status(false, "Error: Table '" + target_.lexeme + "' already exists");
        }

        //insert internal column _rowid
        not_null_constraints_.insert(not_null_constraints_.begin(), true);
        names_.insert(names_.begin(), Token("_rowid", TokenType::Identifier));
        types_.insert(types_.begin(), Token("int8", TokenType::Int8));

        for (size_t i = 0; i < names_.size(); i++) {
            Token name = names_.at(i);
            Token type = types_.at(i);
            if (name.type != TokenType::Identifier) {
                return Status(false, "Error: '" + name.lexeme + "' is not allowed as column name");
            }
            if (!TokenTypeValidDataType(type.type)) {
                return Status(false, "Error: '" + type.lexeme + "' is not a valid data type");
            }
        }

        if (primary_keys_.empty())
            primary_keys_.push_back(Token("_rowid", TokenType::Identifier));
        
        Table table(target_.lexeme, names_, types_, not_null_constraints_, primary_keys_);
        for (Token t: primary_keys_) {
            if (table.GetAttrIdx(t.lexeme) == -1) {
                return Status(false, "Error: Column '" + t.lexeme + "' not declared in table '" + target_.lexeme + "'");
            } 

            //set 'not null' for primary key columns in case not already set
            not_null_constraints_.at(table.GetAttrIdx(t.lexeme)) = true;
        }

        //TODO: verify that foreign table in foreign keys exists
        //TODO: verify that foreign columns in foreign keys exist

        return Status(true, "ok");
    }

    Status Execute() override {
        //put able in catalogue
        Table table(target_.lexeme, names_, types_, not_null_constraints_, primary_keys_);
        GetQueryState()->db->Catalogue()->Put(rocksdb::WriteOptions(), target_.lexeme, table.Serialize());

        //create indexes
        GetQueryState()->db->CreateIdxHandle(table.PrimaryIdxName());

        return Status(true, "CREATE TABLE");
    }
    std::string ToString() override {
        return "create";
    }
private:
    Token target_;
    std::vector<Token> names_;
    std::vector<Token> types_;
    std::vector<bool> not_null_constraints_;
    std::vector<Token> primary_keys_;
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
        WorkingAttributeSet* working_attrs;
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

            if (!TokenTypeIsInteger(type)) {
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
            bool is_agg;
            
            GetQueryState()->PushScopeRow(r);
            Status s = where_clause_->Eval(r, &d, &is_agg);
            GetQueryState()->PopScopeRow();

            if (!s.Ok()) 
                return s;

            if (!d.AsBool())
                continue;

            rs->rows_.push_back(r);
        }

        //sort filtered rows in-place
        if (!order_cols_.empty()) {
            std::vector<OrderCol>& order_cols = order_cols_; //lambdas can only capture non-member variable
            QueryState* qs = GetQueryState();

            std::sort(rs->rows_.begin(), rs->rows_.end(), 
                        //No error checking in lambda...
                        //do scope rows need to be pushed/popped of query state stack here???
                        [order_cols, qs](Row* t1, Row* t2) -> bool { 
                            for (OrderCol oc: order_cols) {
                                Datum d1;
                                bool t1_agg;
                                qs->PushScopeRow(t1);
                                oc.col->Eval(t1, &d1, &t1_agg);
                                qs->PopScopeRow();

                                Datum d2;
                                bool t2_agg;
                                qs->PushScopeRow(t2);
                                oc.col->Eval(t2, &d2, &t2_agg);
                                qs->PopScopeRow();

                                if (d1 == d2)
                                    continue;

                                Row* r = nullptr;
                                Datum d;
                                bool asc_agg;
                                oc.asc->Eval(r, &d, &asc_agg);
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
        for (size_t i = 0; i < rs->rows_.size(); i++) {
            proj_rs->rows_.push_back(new Row({}));
        }

        for (Expr* e: projs_) {
            std::vector<Datum> col;
            Datum result;
            bool is_agg;

            for (Row* r: rs->rows_) {
                GetQueryState()->PushScopeRow(r);
                Status s = e->Eval(r, &result, &is_agg);
                GetQueryState()->PopScopeRow();

                if (!s.Ok())
                    return s;
                if (!is_agg)
                    col.push_back(result);
            }
            if (is_agg) {
                col.push_back(result);
                GetQueryState()->ResetAggState();
            }

            if (col.size() < proj_rs->rows_.size()) {
                proj_rs->rows_.resize(col.size());
            } else if (col.size() > proj_rs->rows_.size()) {
                return Status(false, "Error: Mixing column references with and without aggregation functions causes mismatched column sizes!");
            }

            for (size_t i = 0; i < col.size(); i++) {
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
        bool dummy_agg;
        //do rows need to be pushed/popped on query state row stack here?
        //should scalar subqueries be allowed in the limit clause?
        Status s = limit_->Eval(&dummy_row, &d, &dummy_agg);
        if (!s.Ok()) return s;

        size_t limit = d == -1 ? std::numeric_limits<size_t>::max() : WSLDB_INTEGER_LITERAL(d);
        if (limit < final_rs->rows_.size()) {
            final_rs->rows_.resize(limit);
        }

        return Status(true, "(" + std::to_string(final_rs->rows_.size()) + " rows)", final_rs);
    }
    std::string ToString() override {
        std::string result = "select ";
        for (Expr* e: projs_) {
            result += e->ToString() + " ";
        }
        //result += "from " + target_->ToString();
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

class InsertStmt: public Stmt {
public:
    InsertStmt(QueryState* qs, Token target, std::vector<std::vector<Expr*>> col_assigns):
        Stmt(qs), target_(target), col_assigns_(std::move(col_assigns)) {}

    Status Analyze(std::vector<TokenType>& types) override {
        Status s = OpenTable(target_.lexeme, &table_);
        if (!s.Ok())
            return s;
       
        WorkingAttributeSet* working_attrs = new WorkingAttributeSet(table_, target_.lexeme); //does postgresql allow table aliases on insert?

        GetQueryState()->PushAnalysisScope(working_attrs);

        for (const std::vector<Expr*>& assigns: col_assigns_) {
            for (Expr* e: assigns) {
                TokenType type;
                Status s = e->Analyze(GetQueryState(), &type);
                if (!s.Ok())
                    return s;
            }
        }

        GetQueryState()->PopAnalysisScope();

        return Status(true, "ok");
    }

    Status Execute() override {
        for (std::vector<Expr*> exprs: col_assigns_) {
            //fill call fields with default null
            std::vector<Datum> nulls;
            for (size_t i = 0; i < table_->Attrs().size(); i++) {
                nulls.push_back(Datum());
            }
            Row row(nulls);

            for (Expr* e: exprs) {
                Datum d;
                bool is_agg;
                Status s = e->Eval(&row, &d, &is_agg); //result d is not used
                if (!s.Ok()) return s;
            }

            Status s = table_->Insert(GetQueryState()->db, row.data_);
            if (!s.Ok())
                return s;
        }

        return Status(true, "INSERT " + std::to_string(col_assigns_.size()));

    }
    std::string ToString() override {
        return "insert";
    }
private:
    Token target_;
    Table* table_;
    std::vector<std::vector<Expr*>> col_assigns_;
};

class UpdateStmt: public Stmt {
public:
    UpdateStmt(QueryState* qs, Token target, std::vector<Expr*> assigns, Expr* where_clause):
        Stmt(qs), target_(target), assigns_(std::move(assigns)), where_clause_(where_clause) {}

    Status Analyze(std::vector<TokenType>& types) override {
        Status s = OpenTable(target_.lexeme, &table_);
        if (!s.Ok())
            return s;
       
        WorkingAttributeSet* working_attrs = new WorkingAttributeSet(table_, target_.lexeme); //Does postgresql allow table aliases on update?

        GetQueryState()->PushAnalysisScope(working_attrs);

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
        table_->BeginScan(GetQueryState()->db);

        Row* r;
        int update_count = 0;
        Datum d;
        while (table_->NextRow(GetQueryState()->db, &r).Ok()) {
            bool is_agg;
            GetQueryState()->PushScopeRow(r);
            Status s = where_clause_->Eval(r, &d, &is_agg);
            GetQueryState()->PopScopeRow();

            if (!s.Ok()) 
                return s;

            if (!d.AsBool())
                continue;

            for (Expr* e: assigns_) {
                bool is_agg;
                GetQueryState()->PushScopeRow(r);
                Status s = e->Eval(r, &d, &is_agg); //returned Datum of ColAssign expressions are ignored
                GetQueryState()->PopScopeRow();

                if (!s.Ok()) 
                    return s;
            }
            table_->UpdatePrev(GetQueryState()->db, r);
            update_count++;
        }

        return Status(true, "(" + std::to_string(update_count) + " deletes)");
    }
    std::string ToString() override {
        return "update";
    }
private:
    Token target_;
    Table* table_;
    std::vector<Expr*> assigns_;
    Expr* where_clause_;
};

class DeleteStmt: public Stmt {
public:
    DeleteStmt(QueryState* qs, Token target, Expr* where_clause): 
        Stmt(qs), target_(target), where_clause_(where_clause) {}

    Status Analyze(std::vector<TokenType>& types) override {
        Status s = OpenTable(target_.lexeme, &table_);
        if (!s.Ok())
            return s;
       
        WorkingAttributeSet* working_attrs = new WorkingAttributeSet(table_, target_.lexeme); //Does postgresql allow table aliases on delete?

        GetQueryState()->PushAnalysisScope(working_attrs);

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
        table_->BeginScan(GetQueryState()->db);

        Row* r;
        int delete_count = 0;
        Datum d;
        while (table_->NextRow(GetQueryState()->db, &r).Ok()) {
            bool is_agg;

            GetQueryState()->PushScopeRow(r);
            Status s = where_clause_->Eval(r, &d, &is_agg);
            GetQueryState()->PopScopeRow();

            if (!s.Ok()) 
                return s;

            if (!d.AsBool())
                continue;

            table_->DeletePrev(GetQueryState()->db);
            delete_count++;
        }

        return Status(true, "(" + std::to_string(delete_count) + " deletes)");
    }
    std::string ToString() override {
        return "delete";
    }
private:
    Token target_;
    Table* table_;
    Expr* where_clause_;
};

class DropTableStmt: public Stmt {
public:
    DropTableStmt(QueryState* qd, Token target_relation, bool has_if_exists):
        Stmt(qd), target_relation_(target_relation), has_if_exists_(has_if_exists), table_(nullptr) {}

    Status Analyze(std::vector<TokenType>& types) override {
        Status s = OpenTable(target_relation_.lexeme, &table_);
        if (!s.Ok() && !has_if_exists_)
            return s;

        return Status(true, "ok");
    }
    Status Execute() override {
        //drop table name from catalogue
        bool idx_existed = GetQueryState()->db->Catalogue()->Delete(rocksdb::WriteOptions(), target_relation_.lexeme).ok();
        if (!idx_existed)
            return Status(true, "(table '" + target_relation_.lexeme + "' doesn't exist and not dropped)");

        //TODO: go through all indexes and drop them here
        //TODO: should get full index name here before passing to DropIdxHandle
        if (table_)
            GetQueryState()->db->DropIdxHandle(table_->PrimaryIdxName());

        return Status(true, "(table '" + target_relation_.lexeme + "' dropped)");
    }
    std::string ToString() override {
        return "drop table";
    }
private:
    Token target_relation_;
    bool has_if_exists_;
    Table* table_;
};

class DescribeTableStmt: public Stmt {
public:
    DescribeTableStmt(QueryState* qd, Token target_relation): Stmt(qd), target_relation_(target_relation), table_(nullptr) {}
    Status Analyze(std::vector<TokenType>& types) override {
        Status s = OpenTable(target_relation_.lexeme, &table_);
        if (!s.Ok())
            return s;

        return Status(true, "ok");
    }
    Status Execute() override {
        std::vector<Datum> tuplefields;
        std::string col = "Column";
        tuplefields.emplace_back(col);
        std::string type = "Type";
        tuplefields.emplace_back(type);
        std::string not_null = "Not Null";
        tuplefields.emplace_back(not_null);
        RowSet* rowset = new RowSet();
        rowset->rows_.push_back(new Row(tuplefields));

        for (const Attribute& a: table_->Attrs()) {
            std::vector<Datum> data = { Datum(a.name), Datum(TokenTypeToString(a.type)), Datum(a.not_null_constraint) };
            rowset->rows_.push_back(new Row(data));
        }

        std::vector<Datum> pk_data = table_->PrimaryKeyFields();
        rowset->rows_.push_back(new Row(pk_data));

        return Status(true, "table '" + target_relation_.lexeme + "'", rowset);
    }
    std::string ToString() override {
        return "describe table";
    }

private:
    Token target_relation_;
    Table* table_;
};

}

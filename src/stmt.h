#pragma once

#include <iostream>
#include <unordered_map>
#include <limits>

#include "expr.h"
#include "token.h"
#include "table.h"
#include "status.h"
#include "rocksdb/db.h"

namespace wsldb {

//Moved class Stmt declaration to expr.h since we need it there,
//but including stmt.h would cause a circular dependency

struct Txn {
    Txn(std::vector<Stmt*> stmts): stmts(std::move(stmts)) {}
    std::vector<Stmt*> stmts;
};

class CreateStmt: public Stmt {
public:
    CreateStmt(Token target, 
               std::vector<Token> names, 
               std::vector<Token> types, 
               std::vector<bool> not_null_constraints, 
               std::vector<Token> primary_keys,
               std::vector<std::vector<Token>> uniques,
               std::vector<bool> nulls_distinct):
                    target_(target), 
                    names_(std::move(names)), 
                    types_(std::move(types)), 
                    not_null_constraints_(std::move(not_null_constraints)), 
                    uniques_(std::move(uniques)) {

        //insert internal column _rowid
        not_null_constraints_.insert(not_null_constraints_.begin(), true);
        names_.insert(names_.begin(), Token("_rowid", TokenType::Identifier));
        types_.insert(types_.begin(), Token("int8", TokenType::Int8));

        //use _rowid if user doesn't specify primary key
        if (primary_keys.empty())
            primary_keys.push_back(Token("_rowid", TokenType::Identifier));

        for (size_t i = 0; i < uniques_.size(); i++) {
            std::vector<Token>& cols = uniques_.at(i);

            //add _rowid to unique column group with 'nulls distinct' clause to differentiate between null fields
            if (nulls_distinct.at(i))
                cols.push_back(Token("_rowid", TokenType::Identifier));
        }

        //an index is created on all uniques, so primary key is decomposed into the 'unique' + 'not null' constraints
        //automatically apply 'not null' constraint to columns in column group of primary key
        for (size_t i = 0; i < names_.size(); i++) {
            if (TokenIn(names_.at(i), primary_keys))
                not_null_constraints_.at(i) = true;
        }

        //insert primary key column group into uniques
        uniques_.insert(uniques_.begin(), primary_keys);
    }

    Status Analyze(QueryState& qs, std::vector<TokenType>& types) override {
        Table* table_ptr;
        Status s = OpenTable(qs, target_.lexeme, &table_ptr);
        if (s.Ok()) {
            return Status(false, "Error: Table '" + target_.lexeme + "' already exists");
        }

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

        for (size_t i = 0; i < uniques_.size(); i++) {
            std::vector<Token>& cols = uniques_.at(i);
            if (!TokensSubsetOf(cols, names_))
                return Status(false, "Error: Referenced column not in table declaration");
        }

        return Status(true, "ok");
    }

    Status Execute(QueryState& qs) override {
        //put able in catalogue
        Table table(target_.lexeme, names_, types_, not_null_constraints_, uniques_);
        qs.storage->Catalogue()->Put(rocksdb::WriteOptions(), target_.lexeme, table.Serialize());

        //GetQueryState()->db->CreateIdxHandle(table.Idx(0).name_);
        for (const Index& i: table.idxs_) {
            qs.storage->CreateIdxHandle(i.name_);
        }
        //TODO: create secondary indexes here

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
    std::vector<std::vector<Token>> uniques_;
};

class SelectStmt: public Stmt {
public:
    SelectStmt(WorkTable* target, 
               std::vector<Expr*> projs,
               Expr* where_clause, 
               std::vector<OrderCol> order_cols,
               Expr* limit,
               bool remove_duplicates):
                    target_(target),
                    projs_(std::move(projs)),
                    where_clause_(where_clause),
                    order_cols_(std::move(order_cols)),
                    limit_(limit),
                    remove_duplicates_(remove_duplicates) {}

    Status Analyze(QueryState& qs, std::vector<TokenType>& types) override {
        WorkingAttributeSet* working_attrs;
        {
            Status s = target_->Analyze(qs, &working_attrs);
            if (!s.Ok())
                return s;
            qs.PushAnalysisScope(working_attrs);
        }

        //projection
        for (Expr* e: projs_) {
            TokenType type;
            Status s = e->Analyze(qs, &type);
            if (!s.Ok()) {
                return s;
            }
            types.push_back(type);
        }

        //where clause
        {
            TokenType type;
            Status s = where_clause_->Analyze(qs, &type);
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
                Status s = oc.col->Analyze(qs, &type);
                if (!s.Ok())
                    return s;
            }

            {
                TokenType type;
                Status s = oc.asc->Analyze(qs, &type);
                if (!s.Ok()) {
                    return s;
                }
            }
        }

        //limit
        {
            TokenType type;
            Status s = limit_->Analyze(qs, &type);
            if (!s.Ok()) {
                return s;
            }

            if (!TokenTypeIsInteger(type)) {
                return Status(false, "Error: 'Limit' must be followed by an expression that evaluates to an integer");
            }
        }

        qs.PopAnalysisScope();

        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        RowSet* rs = new RowSet();
        target_->BeginScan(qs);

        Row* r;
        while (target_->NextRow(qs, &r).Ok()) {
            Datum d;
            
            qs.PushScopeRow(r);
            Status s = where_clause_->Eval(qs, r, &d);
            qs.PopScopeRow();

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
                        //do scope rows need to be pushed/popped of query state stack here???
                        [order_cols, &qs](Row* t1, Row* t2) -> bool { 
                            for (OrderCol oc: order_cols) {
                                Datum d1;
                                qs.PushScopeRow(t1);
                                oc.col->Eval(qs, t1, &d1);
                                qs.PopScopeRow();

                                Datum d2;
                                qs.PushScopeRow(t2);
                                oc.col->Eval(qs, t2, &d2);
                                qs.PopScopeRow();

                                if (d1 == d2)
                                    continue;

                                Row* r = nullptr;
                                Datum d;
                                oc.asc->Eval(qs, r, &d);
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

            for (Row* r: rs->rows_) {
                qs.PushScopeRow(r);
                Status s = e->Eval(qs, r, &result);
                qs.PopScopeRow();

                if (!s.Ok())
                    return s;
                if (!qs.is_agg)
                    col.push_back(result);
            }

            if (qs.is_agg) {
                col.push_back(result);
                qs.ResetAggState();
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
        //do rows need to be pushed/popped on query state row stack here?
        //should scalar subqueries be allowed in the limit clause?
        Status s = limit_->Eval(qs, &dummy_row, &d);
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
    InsertStmt(Token target, std::vector<std::vector<Expr*>> col_assigns):
        target_(target), col_assigns_(std::move(col_assigns)) {}

    Status Analyze(QueryState& qs, std::vector<TokenType>& types) override {
        Status s = OpenTable(qs, target_.lexeme, &table_);
        if (!s.Ok())
            return s;
       
        WorkingAttributeSet* working_attrs = new WorkingAttributeSet(table_, target_.lexeme); //does postgresql allow table aliases on insert?

        qs.PushAnalysisScope(working_attrs);

        for (const std::vector<Expr*>& assigns: col_assigns_) {
            for (Expr* e: assigns) {
                TokenType type;
                Status s = e->Analyze(qs, &type);
                if (!s.Ok())
                    return s;
            }
        }

        qs.PopAnalysisScope();

        return Status(true, "ok");
    }

    Status Execute(QueryState& qs) override {
        for (std::vector<Expr*> exprs: col_assigns_) {
            //fill call fields with default null
            std::vector<Datum> nulls;
            for (size_t i = 0; i < table_->Attrs().size(); i++) {
                nulls.push_back(Datum());
            }
            Row row(nulls);

            for (Expr* e: exprs) {
                Datum d;
                Status s = e->Eval(qs, &row, &d); //result d is not used
                if (!s.Ok()) return s;
            }

            Status s = table_->Insert(qs.storage, row.data_);
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
    UpdateStmt(Token target, std::vector<Expr*> assigns, Expr* where_clause):
        target_(target), assigns_(std::move(assigns)), where_clause_(where_clause) {}

    Status Analyze(QueryState& qs, std::vector<TokenType>& types) override {
        Status s = OpenTable(qs, target_.lexeme, &table_);
        if (!s.Ok())
            return s;
       
        WorkingAttributeSet* working_attrs = new WorkingAttributeSet(table_, target_.lexeme); //Does postgresql allow table aliases on update?

        qs.PushAnalysisScope(working_attrs);

        {
            TokenType type;
            Status s = where_clause_->Analyze(qs, &type);
            if (!s.Ok())
                return s;
            if (type != TokenType::Bool)
                return Status(false, "Error: 'where' clause must evaluated to a boolean value");
        }

        for (Expr* e: assigns_) {
            TokenType type;
            Status s = e->Analyze(qs, &type);
            if (!s.Ok())
                return s;
        }

        qs.PopAnalysisScope();
        
        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        table_->BeginScan(qs.storage);

        Row* r;
        int update_count = 0;
        Datum d;
        while (table_->NextRow(qs.storage, &r).Ok()) {
            qs.PushScopeRow(r);
            Status s = where_clause_->Eval(qs, r, &d);
            qs.PopScopeRow();

            if (!s.Ok()) 
                return s;

            if (!d.AsBool())
                continue;

            for (Expr* e: assigns_) {
                qs.PushScopeRow(r);
                Status s = e->Eval(qs, r, &d); //returned Datum of ColAssign expressions are ignored
                qs.PopScopeRow();

                if (!s.Ok()) 
                    return s;
            }
            table_->UpdatePrev(qs.storage, r);
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
    DeleteStmt(Token target, Expr* where_clause): 
        target_(target), where_clause_(where_clause) {}

    Status Analyze(QueryState& qs, std::vector<TokenType>& types) override {
        Status s = OpenTable(qs, target_.lexeme, &table_);
        if (!s.Ok())
            return s;
       
        WorkingAttributeSet* working_attrs = new WorkingAttributeSet(table_, target_.lexeme); //Does postgresql allow table aliases on delete?

        qs.PushAnalysisScope(working_attrs);

        {
            TokenType type;
            Status s = where_clause_->Analyze(qs, &type);
            if (!s.Ok())
                return s;
            if (type != TokenType::Bool)
                return Status(false, "Error: 'where' clause must evaluated to a boolean value");
        }

        qs.PopAnalysisScope();

        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        table_->BeginScan(qs.storage);

        Row* r;
        int delete_count = 0;
        Datum d;
        while (table_->NextRow(qs.storage, &r).Ok()) {

            qs.PushScopeRow(r);
            Status s = where_clause_->Eval(qs, r, &d);
            qs.PopScopeRow();

            if (!s.Ok()) 
                return s;

            if (!d.AsBool())
                continue;

            table_->DeletePrev(qs.storage);
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
    DropTableStmt(Token target_relation, bool has_if_exists):
        target_relation_(target_relation), has_if_exists_(has_if_exists), table_(nullptr) {}

    Status Analyze(QueryState& qs, std::vector<TokenType>& types) override {
        Status s = OpenTable(qs, target_relation_.lexeme, &table_);
        if (!s.Ok() && !has_if_exists_)
            return s;

        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        //drop table name from catalogue
        bool idx_existed = qs.storage->Catalogue()->Delete(rocksdb::WriteOptions(), target_relation_.lexeme).ok();
        if (!idx_existed)
            return Status(true, "(table '" + target_relation_.lexeme + "' doesn't exist and not dropped)");

        //skip if table doesn't exist - error should be reported in the semantic analysis stage if missing table is error
        if (table_) {
            for (const Index& i: table_->idxs_) {
                qs.storage->DropIdxHandle(i.name_);
            }
        }

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
    DescribeTableStmt(Token target_relation): target_relation_(target_relation), table_(nullptr) {}
    Status Analyze(QueryState& qs, std::vector<TokenType>& types) override {
        Status s = OpenTable(qs, target_relation_.lexeme, &table_);
        if (!s.Ok())
            return s;

        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        RowSet* rowset = new RowSet();

        std::string not_null = "not null";
        for (const Attribute& a: table_->Attrs()) {
            std::vector<Datum> data = { Datum(a.name), Datum(TokenTypeToString(a.type)) };
            if (a.not_null_constraint) {
                data.emplace_back(not_null);
            }
            rowset->rows_.push_back(new Row(data));
        }

        for (const Index& i: table_->idxs_) {
            std::vector<Datum> index_name = {Datum(i.name_)};
            rowset->rows_.push_back(new Row(index_name));
        }

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

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
    Txn(std::vector<Stmt*> stmts, bool commit_on_success): stmts(std::move(stmts)), commit_on_success(commit_on_success) {}
    std::vector<Stmt*> stmts;
    bool commit_on_success;
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

    Status Analyze(QueryState& qs, std::vector<DatumType>& types) override {
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
        //put table in catalogue
        Table table(target_.lexeme, names_, types_, not_null_constraints_, uniques_);

        int default_column_family_idx = 0;
        TableHandle catalogue = qs.storage->GetTableHandle(qs.storage->CatalogueTableName());
        qs.batch->Put(qs.storage->CatalogueTableName(), catalogue.cfs.at(default_column_family_idx), target_.lexeme, table.Serialize()); 

        qs.storage->CreateTableHandle(target_.lexeme, table.idxs_);

        return Status(true, "CREATE TABLE");
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
                    remove_duplicates_(remove_duplicates),
                    row_description_({}) {}

    Status Analyze(QueryState& qs, std::vector<DatumType>& types) override {
        WorkingAttributeSet* working_attrs;
        {
            Status s = target_->Analyze(qs, &working_attrs);
            if (!s.Ok())
                return s;
            qs.PushAnalysisScope(working_attrs);
        }

        //projection
        for (Expr* e: projs_) {
            DatumType type;
            Status s = e->Analyze(qs, &type);
            if (!s.Ok()) {
                return s;
            }
            types.push_back(type);

            //fill in row description for usage during execution stage
            row_description_.emplace_back(e->ToString(), type, false);
        }

        //where clause
        {
            DatumType type;
            Status s = where_clause_->Analyze(qs, &type);
            if (!s.Ok()) {
                return s;
            }

            if (type != DatumType::Bool) {
                return Status(false, "Error: Where clause must be a boolean expression");
            }
        }

        //order cols
        for (OrderCol oc: order_cols_) {
            {
                DatumType type;
                Status s = oc.col->Analyze(qs, &type);
                if (!s.Ok())
                    return s;
            }

            {
                DatumType type;
                Status s = oc.asc->Analyze(qs, &type);
                if (!s.Ok()) {
                    return s;
                }
            }
        }

        //limit
        {
            DatumType type;
            Status s = limit_->Analyze(qs, &type);
            if (!s.Ok()) {
                return s;
            }

            if (!Datum::TypeIsInteger(type)) {
                return Status(false, "Error: 'Limit' must be followed by an expression that evaluates to an integer");
            }
        }

        qs.PopAnalysisScope();

        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        //scan table(s) and filter using 'where' clause
        RowSet* rs = new RowSet(row_description_);
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
        RowSet* proj_rs = new RowSet(row_description_);

        for (size_t i = 0; i < rs->rows_.size(); i++) {
            proj_rs->rows_.push_back(new Row({}));
        }

        int idx = 0;

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

                //if aggregate function, determine the DatumType here (DatumType::Null is used as placeholder in Analyze)
                Attribute a = row_description_.at(idx);
                row_description_.at(idx) = Attribute(a.name, result.Type(), a.not_null_constraint);

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

            idx++;
        }

        //remove duplicates
        RowSet* final_rs = new RowSet(row_description_);
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
            final_rs->rows_ = proj_rs->rows_;
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

        for (const Attribute& a: final_rs->attrs_) {
            std::cout << a.ToString() << std::endl;
        }

        return Status(true, "(" + std::to_string(final_rs->rows_.size()) + " rows)", {final_rs});
    }
private:
    WorkTable* target_;
    std::vector<Expr*> projs_;
    Expr* where_clause_;
    std::vector<OrderCol> order_cols_;
    Expr* limit_;
    bool remove_duplicates_;
    std::vector<Attribute> row_description_;
};

class InsertStmt: public Stmt {
public:
    InsertStmt(Token target, std::vector<Token> attrs, std::vector<std::vector<Expr*>> values):
        target_(target), attrs_(std::move(attrs)), values_(std::move(values)), col_assigns_({}) {}

    Status Analyze(QueryState& qs, std::vector<DatumType>& types) override {
        Status s = OpenTable(qs, target_.lexeme, &table_);
        if (!s.Ok())
            return s;
       
        WorkingAttributeSet* working_attrs = new WorkingAttributeSet(table_, target_.lexeme); //does postgresql allow table aliases on insert?

        for (Token t: attrs_) {
            if (!working_attrs->Contains(target_.lexeme, t.lexeme)) {
                return Status(false, ("Error: Table does not have a matching column name"));
            }
        }

        size_t attr_count = working_attrs->WorkingAttributeCount() - 1; //ignore _rowid since caller doesn't explicitly insert that

        for (const std::vector<Expr*>& tuple: values_) {
            if (attr_count < tuple.size())
                return Status(false, "Error: Value count must match specified attribute name count");

            std::vector<Expr*> col_assign;
            for (size_t i = 0; i < attrs_.size(); i++) {
                col_assign.push_back(new ColAssign(attrs_.at(i), tuple.at(i)));
            }
            col_assigns_.push_back(col_assign);
        }

        qs.PushAnalysisScope(working_attrs);
        for (const std::vector<Expr*>& assigns: col_assigns_) {
            for (Expr* e: assigns) {
                DatumType type;
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

            Status s = table_->Insert(qs.storage, qs.batch, row.data_);
            if (!s.Ok())
                return s;
        }

        return Status(true, "INSERT " + std::to_string(col_assigns_.size()));
    }
private:
    Token target_;
    Table* table_;
    std::vector<Token> attrs_;
    std::vector<std::vector<Expr*>> values_;
    std::vector<std::vector<Expr*>> col_assigns_;
};

class UpdateStmt: public Stmt {
public:
    UpdateStmt(Token target, std::vector<Expr*> assigns, Expr* where_clause):
        target_(target), assigns_(std::move(assigns)), where_clause_(where_clause) {}

    Status Analyze(QueryState& qs, std::vector<DatumType>& types) override {
        Status s = OpenTable(qs, target_.lexeme, &table_);
        if (!s.Ok())
            return s;
       
        WorkingAttributeSet* working_attrs = new WorkingAttributeSet(table_, target_.lexeme); //Does postgresql allow table aliases on update?

        qs.PushAnalysisScope(working_attrs);

        {
            DatumType type;
            Status s = where_clause_->Analyze(qs, &type);
            if (!s.Ok())
                return s;
            if (type != DatumType::Bool)
                return Status(false, "Error: 'where' clause must evaluated to a boolean value");
        }

        for (Expr* e: assigns_) {
            DatumType type;
            Status s = e->Analyze(qs, &type);
            if (!s.Ok())
                return s;
        }

        qs.PopAnalysisScope();
        
        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        int scan_idx = 0;
        table_->BeginScan(qs.storage, scan_idx);

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

            Row updated_row = *r;

            for (Expr* e: assigns_) {
                qs.PushScopeRow(&updated_row);
                Status s = e->Eval(qs, &updated_row, &d); //returned Datum of ColAssign expressions are ignored
                qs.PopScopeRow();

                if (!s.Ok()) 
                    return s;
            }

            table_->UpdateRow(qs.storage, qs.batch, &updated_row, r);
            update_count++;
        }

        return Status(true, "(" + std::to_string(update_count) + " deletes)");
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

    Status Analyze(QueryState& qs, std::vector<DatumType>& types) override {
        Status s = OpenTable(qs, target_.lexeme, &table_);
        if (!s.Ok())
            return s;
       
        WorkingAttributeSet* working_attrs = new WorkingAttributeSet(table_, target_.lexeme); //Does postgresql allow table aliases on delete?

        qs.PushAnalysisScope(working_attrs);

        {
            DatumType type;
            Status s = where_clause_->Analyze(qs, &type);
            if (!s.Ok())
                return s;
            if (type != DatumType::Bool)
                return Status(false, "Error: 'where' clause must evaluated to a boolean value");
        }

        qs.PopAnalysisScope();

        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        int scan_idx = 0;
        table_->BeginScan(qs.storage, scan_idx);

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

            table_->DeleteRow(qs.storage, qs.batch, r);
            delete_count++;
        }

        return Status(true, "(" + std::to_string(delete_count) + " deletes)");
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

    Status Analyze(QueryState& qs, std::vector<DatumType>& types) override {
        Status s = OpenTable(qs, target_relation_.lexeme, &table_);
        if (!s.Ok() && !has_if_exists_)
            return s;

        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        //drop table name from catalogue
        if (!table_) {
            return Status(true, "(table '" + target_relation_.lexeme + "' doesn't exist and not dropped)");
        }

        //skip if table doesn't exist - error should be reported in the semantic analysis stage if missing table is error
        if (table_) {
            int default_column_family_idx = 0;
            TableHandle catalogue = qs.storage->GetTableHandle(qs.storage->CatalogueTableName());
            qs.batch->Delete(qs.storage->CatalogueTableName(), catalogue.cfs.at(default_column_family_idx), target_relation_.lexeme);
            qs.storage->DropTableHandle(target_relation_.lexeme);
        }

        return Status(true, "(table '" + target_relation_.lexeme + "' dropped)");
    }
private:
    Token target_relation_;
    bool has_if_exists_;
    Table* table_;
};

class DescribeTableStmt: public Stmt {
public:
    DescribeTableStmt(Token target_relation): target_relation_(target_relation), table_(nullptr) {}
    Status Analyze(QueryState& qs, std::vector<DatumType>& types) override {
        Status s = OpenTable(qs, target_relation_.lexeme, &table_);
        if (!s.Ok())
            return s;

        return Status(true, "ok");
    }
    Status Execute(QueryState& qs) override {
        //column information
        std::vector<Attribute> row_description = { Attribute("name", DatumType::Text, true), 
                                                   Attribute("type", DatumType::Text, true), 
                                                   Attribute("not null", DatumType::Bool, true) };
        RowSet* rowset = new RowSet(row_description);

        for (const Attribute& a: table_->Attrs()) {
            std::vector<Datum> data = { Datum(a.name), Datum(Datum::TypeToString(a.type)) };
            data.emplace_back(a.not_null_constraint);
            rowset->rows_.push_back(new Row(data));
        }

        //index information
        std::vector<Attribute> idx_row_description = { Attribute("type", DatumType::Text, true),
                                                       Attribute("name", DatumType::Text, true) };

        RowSet* idx_rowset = new RowSet(idx_row_description);

        std::string type = "lsm tree";
        for (const Index& i: table_->idxs_) {
            std::vector<Datum> index_info = { Datum(type), Datum(i.name_) };
            idx_rowset->rows_.push_back(new Row(index_info));
        }

        return Status(true, "table '" + target_relation_.lexeme + "'", { rowset, idx_rowset });
    }
private:
    Token target_relation_;
    Table* table_;
};

}

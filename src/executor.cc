#include "executor.h"

namespace wsldb {

Executor::Executor(Storage* storage, Batch* batch): storage_(storage), batch_(batch), qs_(QueryState(storage, batch)), proj_types_({}) {}

Status Executor::Verify(Stmt* stmt) {
    switch (stmt->Type()) {
        case StmtType::Create:
            return CreateVerifier((CreateStmt*)stmt); //C-style cast since dynamic_cast requires rtti, but rocksdb not currently compiled with rtti
        case StmtType::Insert:
            return InsertVerifier((InsertStmt*)stmt);
        case StmtType::Update:
            return UpdateVerifier((UpdateStmt*)stmt);
        case StmtType::Delete:
            return DeleteVerifier((DeleteStmt*)stmt);
        case StmtType::Select:
            return SelectVerifier((SelectStmt*)stmt);
        case StmtType::DescribeTable:
            return DescribeTableVerifier((DescribeTableStmt*)stmt);
        case StmtType::DropTable:
            return DropTableVerifier((DropTableStmt*)stmt);
        default:
            return Status(false, "Execution Error: Invalid statement type");
    }
}

Status Executor::Execute(Stmt* stmt) {
    switch (stmt->Type()) {
        case StmtType::Create:
            return CreateExecutor((CreateStmt*)stmt);
        case StmtType::Insert:
            return InsertExecutor((InsertStmt*)stmt);
        case StmtType::Update:
            return UpdateExecutor((UpdateStmt*)stmt);
        case StmtType::Delete:
            return DeleteExecutor((DeleteStmt*)stmt);
        case StmtType::Select:
            return SelectExecutor((SelectStmt*)stmt);
        case StmtType::DescribeTable:
            return DescribeTableExecutor((DescribeTableStmt*)stmt);
        case StmtType::DropTable:
            return DropTableExecutor((DropTableStmt*)stmt);
        default:
            return Status(false, "Execution Error: Invalid statement type");
    }
}

Status Executor::CreateVerifier(CreateStmt* stmt) { 
    Table* table_ptr;
    Status s = OpenTable(qs_, stmt->target_.lexeme, &table_ptr);
    if (s.Ok()) {
        return Status(false, "Error: Table '" + stmt->target_.lexeme + "' already exists");
    }

    for (size_t i = 0; i < stmt->names_.size(); i++) {
        Token name = stmt->names_.at(i);
        Token type = stmt->types_.at(i);
        if (name.type != TokenType::Identifier) {
            return Status(false, "Error: '" + name.lexeme + "' is not allowed as column name");
        }
        if (!TokenTypeValidDataType(type.type)) {
            return Status(false, "Error: '" + type.lexeme + "' is not a valid data type");
        }
    }

    for (size_t i = 0; i < stmt->uniques_.size(); i++) {
        std::vector<Token>& cols = stmt->uniques_.at(i);
        if (!TokensSubsetOf(cols, stmt->names_))
            return Status(false, "Error: Referenced column not in table declaration");
    }
    
    return Status();
}

Status Executor::CreateExecutor(CreateStmt* stmt) { 
    Table table(stmt->target_.lexeme, stmt->names_, stmt->types_, stmt->not_null_constraints_, stmt->uniques_);

    //TODO: this should be a single transaction: just an example of how to use
    //rocksdb::TransactionDB.  Should put this in wrapper in storage class
    int default_column_family_idx = 0;
    TableHandle catalogue = qs_.storage->GetTable(qs_.storage->CatalogueTableName());

    rocksdb::WriteOptions options;
    rocksdb::Transaction* txn = catalogue.db->BeginTransaction(options);
    txn->Put(catalogue.cfs.at(default_column_family_idx), stmt->target_.lexeme, table.Serialize());
    txn->Commit();
    delete txn;

    //qs.batch->Put(qs.storage->CatalogueTableName(), catalogue.cfs.at(default_column_family_idx), target_.lexeme, table.Serialize()); 

    qs_.storage->CreateTable(stmt->target_.lexeme, table.idxs_);
    return Status(); 
}

Status Executor::InsertVerifier(InsertStmt* stmt) { 
    Status s = OpenTable(qs_, stmt->target_.lexeme, &stmt->table_);
    if (!s.Ok())
        return s;
   
    WorkingAttributeSet* working_attrs = new WorkingAttributeSet(stmt->table_, stmt->target_.lexeme); //does postgresql allow table aliases on insert?

    for (Token t: stmt->attrs_) {
        if (!working_attrs->Contains(stmt->target_.lexeme, t.lexeme)) {
            return Status(false, ("Error: Table does not have a matching column name"));
        }
    }

    size_t attr_count = working_attrs->WorkingAttributeCount() - 1; //ignore _rowid since caller doesn't explicitly insert that

    for (const std::vector<Expr*>& tuple: stmt->values_) {
        if (attr_count < tuple.size())
            return Status(false, "Error: Value count must match specified attribute name count");

        std::vector<Expr*> col_assign;
        for (size_t i = 0; i < stmt->attrs_.size(); i++) {
            col_assign.push_back(new ColAssign(stmt->attrs_.at(i), tuple.at(i)));
        }
        stmt->col_assigns_.push_back(col_assign);
    }

    qs_.PushAnalysisScope(working_attrs);
    for (const std::vector<Expr*>& assigns: stmt->col_assigns_) {
        for (Expr* e: assigns) {
            DatumType type;
            Status s = e->Analyze(qs_, &type);
            if (!s.Ok())
                return s;
        }
    }
    qs_.PopAnalysisScope();

    return Status(); 
}

Status Executor::InsertExecutor(InsertStmt* stmt) { 
    for (std::vector<Expr*> exprs: stmt->col_assigns_) {
        //fill call fields with default null
        std::vector<Datum> nulls;
        for (size_t i = 0; i < stmt->table_->Attrs().size(); i++) {
            nulls.push_back(Datum());
        }
        Row row(nulls);

        for (Expr* e: exprs) {
            Datum d;
            Status s = e->Eval(qs_, &row, &d); //result d is not used
            if (!s.Ok()) return s;
        }

        Status s = stmt->table_->Insert(qs_.storage, qs_.batch, row.data_);
        if (!s.Ok())
            return s;
    }

    return Status(); 
}

Status Executor::UpdateVerifier(UpdateStmt* stmt) { 
    Status s = OpenTable(qs_, stmt->target_.lexeme, &stmt->table_);
    if (!s.Ok())
        return s;
   
    WorkingAttributeSet* working_attrs = new WorkingAttributeSet(stmt->table_, stmt->target_.lexeme); //Does postgresql allow table aliases on update?

    qs_.PushAnalysisScope(working_attrs);

    {
        DatumType type;
        Status s = stmt->where_clause_->Analyze(qs_, &type);
        if (!s.Ok())
            return s;
        if (type != DatumType::Bool)
            return Status(false, "Error: 'where' clause must evaluated to a boolean value");
    }

    for (Expr* e: stmt->assigns_) {
        DatumType type;
        Status s = e->Analyze(qs_, &type);
        if (!s.Ok())
            return s;
    }

    qs_.PopAnalysisScope();
    return Status(); 
}

Status Executor::UpdateExecutor(UpdateStmt* stmt) { 
    int scan_idx = 0;
    stmt->table_->BeginScan(qs_.storage, scan_idx);

    Row* r;
    int update_count = 0;
    Datum d;
    while (stmt->table_->NextRow(qs_.storage, &r).Ok()) {
        qs_.PushScopeRow(r);
        Status s = stmt->where_clause_->Eval(qs_, r, &d);
        qs_.PopScopeRow();

        if (!s.Ok()) 
            return s;

        if (!d.AsBool())
            continue;

        Row updated_row = *r;

        for (Expr* e: stmt->assigns_) {
            qs_.PushScopeRow(&updated_row);
            Status s = e->Eval(qs_, &updated_row, &d); //returned Datum of ColAssign expressions are ignored
            qs_.PopScopeRow();

            if (!s.Ok()) 
                return s;
        }

        stmt->table_->UpdateRow(qs_.storage, qs_.batch, &updated_row, r);
        update_count++;
    }

    return Status(); 
}

Status Executor::DeleteVerifier(DeleteStmt* stmt) { 
    Status s = OpenTable(qs_, stmt->target_.lexeme, &stmt->table_);
    if (!s.Ok())
        return s;
   
    WorkingAttributeSet* working_attrs = new WorkingAttributeSet(stmt->table_, stmt->target_.lexeme);

    qs_.PushAnalysisScope(working_attrs);

    {
        DatumType type;
        Status s = stmt->where_clause_->Analyze(qs_, &type);
        if (!s.Ok())
            return s;
        if (type != DatumType::Bool)
            return Status(false, "Error: 'where' clause must evaluated to a boolean value");
    }

    qs_.PopAnalysisScope();

    return Status(); 
}

Status Executor::DeleteExecutor(DeleteStmt* stmt) { 
    int scan_idx = 0;
    stmt->table_->BeginScan(qs_.storage, scan_idx);

    Row* r;
    int delete_count = 0;
    Datum d;
    while (stmt->table_->NextRow(qs_.storage, &r).Ok()) {

        qs_.PushScopeRow(r);
        Status s = stmt->where_clause_->Eval(qs_, r, &d);
        qs_.PopScopeRow();

        if (!s.Ok()) 
            return s;

        if (!d.AsBool())
            continue;

        stmt->table_->DeleteRow(qs_.storage, qs_.batch, r);
        delete_count++;
    }
    return Status(); 
}

Status Executor::SelectVerifier(SelectStmt* stmt) { 
    proj_types_.push_back({});
    WorkingAttributeSet* working_attrs;
    {
        Status s = stmt->target_->Analyze(qs_, &working_attrs);
        if (!s.Ok())
            return s;
        qs_.PushAnalysisScope(working_attrs);
    }

    //projection
    for (Expr* e: stmt->projs_) {
        DatumType type;
        Status s = e->Analyze(qs_, &type);
        if (!s.Ok()) {
            return s;
        }
        proj_types_.back().push_back(type);

        //fill in row description for usage during execution stage
        stmt->row_description_.emplace_back(e->ToString(), type, false);
    }

    //where clause
    {
        DatumType type;
        Status s = stmt->where_clause_->Analyze(qs_, &type);
        if (!s.Ok()) {
            return s;
        }

        if (type != DatumType::Bool) {
            return Status(false, "Error: Where clause must be a boolean expression");
        }
    }

    //order cols
    for (OrderCol oc: stmt->order_cols_) {
        {
            DatumType type;
            Status s = oc.col->Analyze(qs_, &type);
            if (!s.Ok())
                return s;
        }

        {
            DatumType type;
            Status s = oc.asc->Analyze(qs_, &type);
            if (!s.Ok()) {
                return s;
            }
        }
    }

    //limit
    {
        DatumType type;
        Status s = stmt->limit_->Analyze(qs_, &type);
        if (!s.Ok()) {
            return s;
        }

        if (!Datum::TypeIsInteger(type)) {
            return Status(false, "Error: 'Limit' must be followed by an expression that evaluates to an integer");
        }
    }

    qs_.PopAnalysisScope();
    proj_types_.pop_back();

    return Status(); 
}

Status Executor::SelectExecutor(SelectStmt* stmt) { 
    //scan table(s) and filter using 'where' clause
    RowSet* rs = new RowSet(stmt->row_description_);
    stmt->target_->BeginScan(qs_);

    Row* r;
    while (stmt->target_->NextRow(qs_, &r).Ok()) {
        Datum d;
        
        qs_.PushScopeRow(r);
        Status s = stmt->where_clause_->Eval(qs_, r, &d);
        qs_.PopScopeRow();

        if (!s.Ok()) 
            return s;

        if (!d.AsBool())
            continue;

        rs->rows_.push_back(r);
    }

    //sort filtered rows in-place
    if (!stmt->order_cols_.empty()) {
        //lambdas can only capture non-member variables
        std::vector<OrderCol>& order_cols = stmt->order_cols_;
        QueryState& qs = qs_;

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
    RowSet* proj_rs = new RowSet(stmt->row_description_);

    for (size_t i = 0; i < rs->rows_.size(); i++) {
        proj_rs->rows_.push_back(new Row({}));
    }

    int idx = 0;

    for (Expr* e: stmt->projs_) {
        std::vector<Datum> col;
        Datum result;

        for (Row* r: rs->rows_) {
            qs_.PushScopeRow(r);
            Status s = e->Eval(qs_, r, &result);
            qs_.PopScopeRow();

            if (!s.Ok())
                return s;

            if (!qs_.is_agg)
                col.push_back(result);
        }

        if (qs_.is_agg) {
            col.push_back(result);

            //if aggregate function, determine the DatumType here (DatumType::Null is used as placeholder in Analyze)
            Attribute a = stmt->row_description_.at(idx);
            stmt->row_description_.at(idx) = Attribute(a.name, result.Type(), a.not_null_constraint);

            qs_.ResetAggState();
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
    RowSet* final_rs = new RowSet(stmt->row_description_);
    if (stmt->remove_duplicates_) {
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
    Status s = stmt->limit_->Eval(qs_, &dummy_row, &d);
    if (!s.Ok()) return s;

    size_t limit = d == -1 ? std::numeric_limits<size_t>::max() : d.AsInt8();
    if (limit < final_rs->rows_.size()) {
        final_rs->rows_.resize(limit);
    }

    return Status(true, "(" + std::to_string(final_rs->rows_.size()) + " rows)", {final_rs});
}

Status Executor::DescribeTableVerifier(DescribeTableStmt* stmt) { 
    Status s = OpenTable(qs_, stmt->target_relation_.lexeme, &stmt->table_);
    if (!s.Ok())
        return s;

    return Status(); 
}

Status Executor::DescribeTableExecutor(DescribeTableStmt* stmt) { 
    //column information
    std::vector<Attribute> row_description = { Attribute("name", DatumType::Text, true), 
                                               Attribute("type", DatumType::Text, true), 
                                               Attribute("not null", DatumType::Bool, true) };
    RowSet* rowset = new RowSet(row_description);

    for (const Attribute& a: stmt->table_->Attrs()) {
        std::vector<Datum> data = { Datum(a.name), Datum(Datum::TypeToString(a.type)) };
        data.emplace_back(a.not_null_constraint);
        rowset->rows_.push_back(new Row(data));
    }

    //index information
    std::vector<Attribute> idx_row_description = { Attribute("type", DatumType::Text, true),
                                                   Attribute("name", DatumType::Text, true) };

    RowSet* idx_rowset = new RowSet(idx_row_description);

    std::string type = "lsm tree";
    for (const Index& i: stmt->table_->idxs_) {
        std::vector<Datum> index_info = { Datum(type), Datum(i.name_) };
        idx_rowset->rows_.push_back(new Row(index_info));
    }

    return Status(true, "table '" + stmt->target_relation_.lexeme + "'", { rowset, idx_rowset });
}

Status Executor::DropTableVerifier(DropTableStmt* stmt) { 
    Status s = OpenTable(qs_, stmt->target_relation_.lexeme, &stmt->table_);
    if (!s.Ok() && !stmt->has_if_exists_)
        return s;

    return Status(); 
}

Status Executor::DropTableExecutor(DropTableStmt* stmt) { 
    //drop table name from catalogue
    if (!stmt->table_) {
        return Status(true, "(table '" + stmt->target_relation_.lexeme + "' doesn't exist and not dropped)");
    }

    //skip if table doesn't exist - error should be reported in the semantic analysis stage if missing table is error
    if (stmt->table_) {
        int default_column_family_idx = 0;
        TableHandle catalogue = qs_.storage->GetTable(qs_.storage->CatalogueTableName());
        qs_.batch->Delete(qs_.storage->CatalogueTableName(), catalogue.cfs.at(default_column_family_idx), stmt->target_relation_.lexeme);
        qs_.storage->DropTable(stmt->target_relation_.lexeme);
    }

    return Status(true, "(table '" + stmt->target_relation_.lexeme + "' dropped)");
}

}

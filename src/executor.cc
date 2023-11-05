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

Status Executor::Verify(Expr* expr, DatumType* type) {
    switch (expr->Type()) {
        case ExprType::Literal:
            return VerifyLiteral((Literal*)expr, type);
        case ExprType::Binary:
            return VerifyBinary((Binary*)expr, type);
        case ExprType::Unary:
            return VerifyUnary((Unary*)expr, type);
        case ExprType::ColRef:
            return VerifyColRef((ColRef*)expr, type);
        case ExprType::ColAssign:
            return VerifyColAssign((ColAssign*)expr, type);
        case ExprType::Call:
            return VerifyCall((Call*)expr, type);
        case ExprType::IsNull:
            return VerifyIsNull((IsNull*)expr, type);
        case ExprType::ScalarSubquery:
            return VerifyScalarSubquery((ScalarSubquery*)expr, type);
        default:
            return Status(false, "Execution Error: Invalid expression type");
    }
}

Status Executor::Eval(Expr* expr, Row* row, Datum* result) {
    switch (expr->Type()) {
        case ExprType::Literal:
            return EvalLiteral((Literal*)expr, row, result);
        case ExprType::Binary:
            return EvalBinary((Binary*)expr, row, result);
        case ExprType::Unary:
            return EvalUnary((Unary*)expr, row, result);
        case ExprType::ColRef:
            return EvalColRef((ColRef*)expr, row, result);
        case ExprType::ColAssign:
            return EvalColAssign((ColAssign*)expr, row, result);
        case ExprType::Call:
            return EvalCall((Call*)expr, row, result);
        case ExprType::IsNull:
            return EvalIsNull((IsNull*)expr, row, result);
        case ExprType::ScalarSubquery:
            return EvalScalarSubquery((ScalarSubquery*)expr, row, result);
        default:
            return Status(false, "Execution Error: Invalid expression type");
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
            Status s = Verify(e, &type);
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
        Status s = Verify(stmt->where_clause_, &type);
        if (!s.Ok())
            return s;
        if (type != DatumType::Bool)
            return Status(false, "Error: 'where' clause must evaluated to a boolean value");
    }

    for (Expr* e: stmt->assigns_) {
        DatumType type;
        Status s = Verify(e, &type);
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
        Status s = Verify(stmt->where_clause_, &type);
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
        Status s = Verify(stmt->target_, &working_attrs);
        if (!s.Ok())
            return s;
        qs_.PushAnalysisScope(working_attrs);
    }

    //projection
    for (Expr* e: stmt->projs_) {
        DatumType type;
        Status s = Verify(e, &type);
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
        Status s = Verify(stmt->where_clause_, &type);
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
            Status s = Verify(oc.col, &type);
            if (!s.Ok())
                return s;
        }

        {
            DatumType type;
            Status s = Verify(oc.asc, &type);
            if (!s.Ok()) {
                return s;
            }
        }
    }

    //limit
    {
        DatumType type;
        Status s = Verify(stmt->limit_, &type);
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

Status Executor::VerifyLiteral(Literal* expr, DatumType* type) { 
    *type = LiteralTokenToDatumType(expr->t_.type);
    return Status(); 
}

Status Executor::VerifyBinary(Binary* expr, DatumType* type) { 
    DatumType left_type;
    {
        Status s = Verify(expr->left_, &left_type);
        if (!s.Ok())
            return s;
    }
    DatumType right_type;
    {
        Status s = Verify(expr->right_, &right_type);
        if (!s.Ok())
            return s;
    }

    if (left_type == DatumType::Null || right_type == DatumType::Null) {
        *type = DatumType::Null;
        return Status();
    }

    switch (expr->op_.type) {
        case TokenType::Equal:
        case TokenType::NotEqual:
        case TokenType::Less:
        case TokenType::LessEqual:
        case TokenType::Greater:
        case TokenType::GreaterEqual:
            if (!(Datum::TypeIsNumeric(left_type) && Datum::TypeIsNumeric(right_type)) && left_type != right_type) {
                    return Status(false, "Error: Equality and relational operands must be same data types");
            }
            *type = DatumType::Bool;
            break;
        case TokenType::Or:
        case TokenType::And:
            if (!(left_type == DatumType::Bool && right_type == DatumType::Bool)) {
                return Status(false, "Error: Logical operator operands must be boolean types");
            }
            *type = DatumType::Bool;
            break;
        case TokenType::Plus:
        case TokenType::Minus:
        case TokenType::Star:
        case TokenType::Slash:
            if (!(Datum::TypeIsNumeric(left_type) && Datum::TypeIsNumeric(right_type))) {
                return Status(false, "Error: The '" + expr->op_.lexeme + "' operator operands must both be a numeric type");
            } 
            *type = left_type;
            break;
        default:
            return Status(false, "Implementation Error: op type not implemented in Binary expr!");
            break;
    }

    return Status(); 
}

Status Executor::VerifyUnary(Unary* expr, DatumType* type) { 
    DatumType right_type;
    {
        Status s = Verify(expr->right_, &right_type);
        if (!s.Ok())
            return s;
    }
    switch (expr->op_.type) {
        case TokenType::Not:
            if (right_type != DatumType::Bool) {
                return Status(false, "Error: 'not' operand must be a boolean type.");
            }
            *type = DatumType::Bool;
            break;
        case TokenType::Minus:
            if (!Datum::TypeIsNumeric(right_type)) {
                return Status(false, "Error: '-' operator operand must be numeric type");
            }
            *type = right_type;
            break;
        default:
            return Status(false, "Implementation Error: op type not implemented in Binary expr!");
            break;
    }

    return Status(); 
}

Status Executor::VerifyColRef(ColRef* expr, DatumType* type) { 
    WorkingAttribute a;
    Status s = qs_.GetWorkingAttribute(&a, expr->table_ref_, expr->t_.lexeme);
    if (!s.Ok())
        return s;

    expr->idx_ = a.idx;
    *type = a.type;
    expr->scope_ = a.scope;

    return Status(); 
}

Status Executor::VerifyColAssign(ColAssign* expr, DatumType* type) { 
    DatumType right_type;
    {
        Status s = Verify(expr->right_, &right_type);
        if (!s.Ok())
            return s;
    }

    WorkingAttribute a;
    {
        std::string table_ref = "";
        Status s = qs_.GetWorkingAttribute(&a, table_ref, expr->col_.lexeme);
        if (!s.Ok())
            return s;
    }

    {
        Status s = a.CheckConstraints(right_type);
        if (!s.Ok())
            return s;
    }

    *type = a.type;
    expr->idx_ = a.idx;
    expr->scope_ = a.scope;

    return Status(); 
}

Status Executor::VerifyCall(Call* expr, DatumType* type) { 
    DatumType arg_type;
    {
        Status s = Verify(expr->arg_, &arg_type);
        if (!s.Ok()) {
            return s;
        }
    }

    switch (expr->fcn_.type) {
        case TokenType::Avg:
            //TODO: need to think about how we want to deal with integer division
            //using floor division now if argument is integer type - what does postgres do?
            *type = arg_type;
            break;
        case TokenType::Count:
            *type = DatumType::Int8;
            break;
        case TokenType::Max:
        case TokenType::Min:
        case TokenType::Sum:
            *type = arg_type;
            break;
        default:
            return Status(false, "Error: Invalid function name");
            break;
    }

    if (!TokenTypeIsAggregateFunction(expr->fcn_.type)) {
        return Status(false, "Error: Function '" + expr->fcn_.lexeme + "' does not exist");
    }

    return Status(); 
}

Status Executor::VerifyIsNull(IsNull* expr, DatumType* type) { 
    *type = DatumType::Bool;

    DatumType left_type;
    Status s = Verify(expr->left_, &left_type);
    if (!s.Ok())
        return s;

    return Status(); 
}

Status Executor::VerifyScalarSubquery(ScalarSubquery* expr, DatumType* type) { 
    {
        Status s = Verify(expr->stmt_);
        if (!s.Ok())
            return s;
    }

    //TODO: this should be taken from GetQueryState()->attrs
    //at this point we don't know if the working table is a constant or physical or other table type
    //so how can we set *evaluated_type for type checking?
    if (proj_types_.size() != 1)
        return Status(false, "Error: Scalar subquery must return a single value");

    *type = proj_types_.back().at(0);

    return Status();
}

Status Executor::EvalLiteral(Literal* expr, Row* row, Datum* result) { return Status(); }
Status Executor::EvalBinary(Binary* expr, Row* row, Datum* result) { return Status(); }
Status Executor::EvalUnary(Unary* expr, Row* row, Datum* result) { return Status(); }
Status Executor::EvalColRef(ColRef* expr, Row* row, Datum* result) { return Status(); }
Status Executor::EvalColAssign(ColAssign* expr, Row* row, Datum* result) { return Status(); }
Status Executor::EvalCall(Call* expr, Row* row, Datum* result) { return Status(); }
Status Executor::EvalIsNull(IsNull* expr, Row* row, Datum* result) { return Status(); }
Status Executor::EvalScalarSubquery(ScalarSubquery* expr, Row* row, Datum* result) { return Status(); }

Status Executor::Verify(WorkTable* scan, WorkingAttributeSet** working_attrs) {
    switch (scan->Type()) {
        case ScanType::Left:
            return VerifyLeft((LeftJoin*)scan, working_attrs);
        case ScanType::Full:
            return VerifyFull((FullJoin*)scan, working_attrs);
        case ScanType::Inner:
            return VerifyInner((InnerJoin*)scan, working_attrs);
        case ScanType::Cross:
            return VerifyCross((CrossJoin*)scan, working_attrs);
        case ScanType::Constant:
            return VerifyConstant((ConstantTable*)scan, working_attrs);
        case ScanType::Table:
            return VerifyTable((PrimaryTable*)scan, working_attrs);
        default:
            return Status(false, "Execution Error: Invalid scan type");
    }
}

Status Executor::VerifyLeft(LeftJoin* scan, WorkingAttributeSet** working_attrs) {
    WorkingAttributeSet* left_attrs;
    {
        Status s = Verify(scan->left_, &left_attrs);
        if (!s.Ok())
            return s;
    }

    WorkingAttributeSet* right_attrs;
    {
        Status s = Verify(scan->right_, &right_attrs);
        if (!s.Ok())
            return s;
    }

    scan->right_attr_count_ = right_attrs->WorkingAttributeCount();

    {
        bool has_duplicate_tables;
        *working_attrs = new WorkingAttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
        scan->attrs_ = *working_attrs;
        if (has_duplicate_tables)
            return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
    }

    qs_.PushAnalysisScope(*working_attrs);
    {
        DatumType type;
        Status s = Verify(scan->condition_, &type);
        if (!s.Ok())
            return s;

        if (type != DatumType::Bool) {
            return Status(false, "Error: Inner join condition must evaluate to a boolean type");
        }
    }
    qs_.PopAnalysisScope();

    return Status();
}

Status Executor::VerifyFull(FullJoin* scan, WorkingAttributeSet** working_attrs) {
    Status s = Verify(scan->right_join_, working_attrs);
    scan->attrs_ = *working_attrs;

    if (!s.Ok())
        return s;

    scan->attr_count_ = (*working_attrs)->WorkingAttributeCount();

    return Status();
}

Status Executor::VerifyInner(InnerJoin* scan, WorkingAttributeSet** working_attrs) {
    WorkingAttributeSet* left_attrs;
    {
        Status s = Verify(scan->left_, &left_attrs);
        if (!s.Ok())
            return s;
    }

    WorkingAttributeSet* right_attrs;
    {
        Status s = Verify(scan->right_, &right_attrs);
        if (!s.Ok())
            return s;
    }

    {
        bool has_duplicate_tables;
        *working_attrs = new WorkingAttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
        scan->attrs_ = *working_attrs;
        if (has_duplicate_tables)
            return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
    }

    //Need to put current attributeset into QueryState temporarily so that Expr::Analyze
    //can use that data to perform semantic analysis for 'on' clause.  Normally Expr::Analyze is only
    //called once entire WorkTable is Analyzed an at least a single WorkingAttributeSet is in QueryState,
    //but InnerJoins have an Expr embedded as part of the WorkTable (the 'on' clause), so this is needed
    //Something similar is done in InnerJoin::NextRow
    qs_.PushAnalysisScope(*working_attrs);
    {
        DatumType type;
        Status s = Verify(scan->condition_, &type);
        if (!s.Ok())
            return s;

        if (type != DatumType::Bool) {
            return Status(false, "Error: Inner join condition must evaluate to a boolean type");
        }
    }
    qs_.PopAnalysisScope();

    return Status(true, "ok");
}

Status Executor::VerifyCross(CrossJoin* scan, WorkingAttributeSet** working_attrs) {
    WorkingAttributeSet* left_attrs;
    {
        Status s = Verify(scan->left_, &left_attrs);
        if (!s.Ok())
            return s;
    }

    WorkingAttributeSet* right_attrs;
    {
        Status s = Verify(scan->right_, &right_attrs);
        if (!s.Ok())
            return s;
    }

    {
        bool has_duplicate_tables;
        *working_attrs = new WorkingAttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
        scan->attrs_ = *working_attrs;
        if (has_duplicate_tables)
            return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
    }

    return Status();
}

Status Executor::VerifyConstant(ConstantTable* scan, WorkingAttributeSet** working_attrs) {
    std::vector<std::string> names;
    std::vector<DatumType> types;
    for (Expr* e: scan->target_cols_) {
        DatumType type;
        Status s = Verify(e, &type);
        if (!s.Ok())
            return s;
        names.push_back("?col?");
        types.push_back(type);
    }

    *working_attrs = new WorkingAttributeSet("?table?", names, types);
    scan->attrs_ = *working_attrs;

    return Status();
}

Status Executor::VerifyTable(PrimaryTable* scan, WorkingAttributeSet** working_attrs) {
    std::string serialized_table;
    TableHandle catalogue = qs_.storage->GetTable(qs_.storage->CatalogueTableName());
    bool ok = catalogue.db->Get(rocksdb::ReadOptions(), scan->tab_name_, &serialized_table).ok();

    if (!ok) {
        return Status(false, "Error: Table '" + scan->tab_name_ + "' does not exist");
    }
    scan->table_ = new Table(scan->tab_name_, serialized_table);

    *working_attrs = new WorkingAttributeSet(scan->table_, scan->ref_name_);
    scan->attrs_ = *working_attrs;

    return Status();
}

}

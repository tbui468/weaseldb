#include "executor.h"

namespace wsldb {

Executor::Executor(Storage* storage, Batch* batch): storage_(storage), batch_(batch), qs_(QueryState()) {}

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

Status Executor::CreateExecutor(CreateStmt* stmt) { 
    Table table(stmt->target_.lexeme, stmt->names_, stmt->types_, stmt->not_null_constraints_, stmt->uniques_);

    //TODO: this should be a single transaction: just an example of how to use
    //rocksdb::TransactionDB.  Should put this in wrapper in storage class
    int default_column_family_idx = 0;
    TableHandle catalogue = storage_->GetTable(storage_->CatalogueTableName());

    rocksdb::WriteOptions options;
    rocksdb::Transaction* txn = catalogue.db->BeginTransaction(options);
    txn->Put(catalogue.cfs.at(default_column_family_idx), stmt->target_.lexeme, table.Serialize());
    txn->Commit();
    delete txn;

    //qs.batch->Put(qs.storage->CatalogueTableName(), catalogue.cfs.at(default_column_family_idx), target_.lexeme, table.Serialize()); 

    storage_->CreateTable(stmt->target_.lexeme, table.idxs_);
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
            Status s = Eval(e, &row, &d); //result d is not used
            if (!s.Ok()) return s;
        }

        Status s = stmt->table_->Insert(storage_, batch_, row.data_);
        if (!s.Ok())
            return s;
    }

    return Status(); 
}

Status Executor::UpdateExecutor(UpdateStmt* stmt) { 
    int scan_idx = 0;
    stmt->table_->BeginScan(storage_, scan_idx);

    Row* r;
    int update_count = 0;
    Datum d;
    while (stmt->table_->NextRow(storage_, &r).Ok()) {
        scopes_.push_back(r);
        Status s = Eval(stmt->where_clause_, r, &d);
        scopes_.pop_back();

        if (!s.Ok()) 
            return s;

        if (!d.AsBool())
            continue;

        Row updated_row = *r;

        for (Expr* e: stmt->assigns_) {
            scopes_.push_back(&updated_row);
            Status s = Eval(e, &updated_row, &d); //returned Datum of ColAssign expressions are ignored
            scopes_.pop_back();

            if (!s.Ok()) 
                return s;
        }

        stmt->table_->UpdateRow(storage_, batch_, &updated_row, r);
        update_count++;
    }

    return Status(); 
}

Status Executor::DeleteExecutor(DeleteStmt* stmt) { 
    int scan_idx = 0;
    stmt->table_->BeginScan(storage_, scan_idx);

    Row* r;
    int delete_count = 0;
    Datum d;
    while (stmt->table_->NextRow(storage_, &r).Ok()) {

        scopes_.push_back(r);
        Status s = Eval(stmt->where_clause_, r, &d);
        scopes_.pop_back();

        if (!s.Ok()) 
            return s;

        if (!d.AsBool())
            continue;

        stmt->table_->DeleteRow(storage_, batch_, r);
        delete_count++;
    }
    return Status(); 
}

Status Executor::SelectExecutor(SelectStmt* stmt) { 
    //scan table(s) and filter using 'where' clause
    RowSet* rs = new RowSet(stmt->row_description_);
    BeginScan(stmt->target_);

    Row* r;
    while (NextRow(stmt->target_, &r).Ok()) {
        Datum d;
        
        scopes_.push_back(r);
        Status s = Eval(stmt->where_clause_, r, &d);
        scopes_.pop_back();

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

        std::sort(rs->rows_.begin(), rs->rows_.end(), 
                    //No error checking in lambda...
                    //do scope rows need to be pushed/popped of query state stack here???
                    [order_cols, this](Row* t1, Row* t2) -> bool { 
                        for (OrderCol oc: order_cols) {
                            Datum d1;
                            this->scopes_.push_back(t1);
                            this->Eval(oc.col, t1, &d1);
                            this->scopes_.pop_back();

                            Datum d2;
                            this->scopes_.push_back(t2);
                            this->Eval(oc.col, t2, &d2);
                            this->scopes_.pop_back();

                            if (d1 == d2)
                                continue;

                            Row* r = nullptr;
                            Datum d;
                            this->Eval(oc.asc, r, &d);
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
            scopes_.push_back(r);
            Status s = Eval(e, r, &result);
            scopes_.pop_back();

            if (!s.Ok())
                return s;

            if (!qs_.is_agg)
                col.push_back(result);
        }

        if (qs_.is_agg) {
            //put the result of the aggregate function onto column
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
    Status s = Eval(stmt->limit_, &dummy_row, &d);
    if (!s.Ok()) return s;

    size_t limit = d == -1 ? std::numeric_limits<size_t>::max() : d.AsInt8();
    if (limit < final_rs->rows_.size()) {
        final_rs->rows_.resize(limit);
    }

    return Status(true, "(" + std::to_string(final_rs->rows_.size()) + " rows)", {final_rs});
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

Status Executor::DropTableExecutor(DropTableStmt* stmt) { 
    //drop table name from catalogue
    if (!stmt->table_) {
        return Status(true, "(table '" + stmt->target_relation_.lexeme + "' doesn't exist and not dropped)");
    }

    //skip if table doesn't exist - error should be reported in the semantic analysis stage if missing table is error
    if (stmt->table_) {
        int default_column_family_idx = 0;
        TableHandle catalogue = storage_->GetTable(storage_->CatalogueTableName());
        batch_->Delete(storage_->CatalogueTableName(), catalogue.cfs.at(default_column_family_idx), stmt->target_relation_.lexeme);
        storage_->DropTable(stmt->target_relation_.lexeme);
    }

    return Status(true, "(table '" + stmt->target_relation_.lexeme + "' dropped)");
}

Status Executor::EvalLiteral(Literal* expr, Row* row, Datum* result) {
    qs_.is_agg = false;
    *result = Datum(LiteralTokenToDatumType(expr->t_.type), expr->t_.lexeme);
    return Status();
}

Status Executor::EvalBinary(Binary* expr, Row* row, Datum* result) { 
    qs_.is_agg = false;

    Datum l;
    {
        Status s = Eval(expr->left_, row, &l);
        if (!s.Ok()) return s;
    }

    Datum r;
    {
        Status s = Eval(expr->right_, row, &r);
        if (!s.Ok()) return s;
    }

    if (l.IsType(DatumType::Null) || r.IsType(DatumType::Null)) {
        *result = Datum();
        return Status();
    }

    switch (expr->op_.type) {
        case TokenType::Equal:          *result = Datum(l == r); break;
        case TokenType::NotEqual:       *result = Datum(l != r); break;
        case TokenType::Less:           *result = Datum(l < r); break;
        case TokenType::LessEqual:      *result = Datum(l <= r); break;
        case TokenType::Greater:        *result = Datum(l > r); break;
        case TokenType::GreaterEqual:   *result = Datum(l >= r); break;
        case TokenType::Plus:           *result = Datum(l + r); break;
        case TokenType::Minus:          *result = Datum(l - r); break;
        case TokenType::Star:           *result = Datum(l * r); break;
        case TokenType::Slash:          *result = Datum(l / r); break;
        case TokenType::Or:             *result = Datum(l || r); break;
        case TokenType::And:            *result = Datum(l && r);break;
        default:                        return Status(false, "Error: Invalid binary operator");
    }

    return Status();
}

Status Executor::EvalUnary(Unary* expr, Row* row, Datum* result) {
    qs_.is_agg = false;

    Datum right;
    Status s = Eval(expr->right_, row, &right);
    if (!s.Ok()) return s;

    switch (expr->op_.type) {
        case TokenType::Minus: {
            if (Datum::TypeIsInteger(right.Type())) {
                *result = Datum(static_cast<int64_t>(-WSLDB_NUMERIC_LITERAL(right)));
            } else {
                *result = Datum(static_cast<float>(-WSLDB_NUMERIC_LITERAL(right)));
            }
            break;
        }
        case TokenType::Not:
            *result = Datum(!right.AsBool());
            break;
        default:
            return Status(false, "Error: Invalid unary operator");
    }

    return Status();
}

Status Executor::EvalColRef(ColRef* expr, Row* row, Datum* result) {
    qs_.is_agg = false;
    *result = scopes_.rbegin()[expr->scope_]->data_.at(expr->idx_);
    return Status();
}

Status Executor::EvalColAssign(ColAssign* expr, Row* row, Datum* result) {
    qs_.is_agg = false;

    Datum right;
    Status s = Eval(expr->right_, row, &right);
    if (!s.Ok()) return s;

    row->data_.at(expr->idx_) = right;

    *result = right; //result not used
    return Status();
}

Status Executor::EvalCall(Call* expr, Row* row, Datum* result) {
    Datum arg;
    Status s = Eval(expr->arg_, row, &arg);
    if (!s.Ok())
        return s;

    switch (expr->fcn_.type) {
        case TokenType::Avg:
            qs_.sum_ += arg;
            qs_.count_ += Datum(static_cast<int64_t>(1));
            *result = qs_.sum_ / qs_.count_;
            break;
        case TokenType::Count: {
            qs_.count_ += Datum(static_cast<int64_t>(1));
            *result = qs_.count_;
            break;
        }
        case TokenType::Max:
            if (qs_.first_ || arg > qs_.max_) {
                qs_.max_ = arg;
                qs_.first_ = false;
            }
            *result = qs_.max_;
            break;
        case TokenType::Min:
            if (qs_.first_ || arg < qs_.min_) {
                qs_.min_ = arg;
                qs_.first_ = false;
            }
            *result = qs_.min_;
            break;
        case TokenType::Sum:
            qs_.sum_ += arg;
            *result = qs_.sum_;
            break;
        default:
            return Status(false, "Error: Invalid function name");
            break;
    }

    //Calling Eval on argument may set qs.is_agg to false, so need to set it after evaluating argument
    qs_.is_agg = true;

    return Status();
}

Status Executor::EvalIsNull(IsNull* expr, Row* row, Datum* result) {
    qs_.is_agg = false;

    Datum d;
    Status s = Eval(expr->left_, row, &d);
    if (!s.Ok())
        return s;

    if (d.IsType(DatumType::Null)) {
        *result = Datum(true);
    } else {
        *result = Datum(false);
    }

    return Status();
}

Status Executor::EvalScalarSubquery(ScalarSubquery* expr, Row* row, Datum* result) {
    qs_.is_agg = false;

    Status s = Execute(expr->stmt_);
    if (!s.Ok())
        return s;

    if (s.Tuples().empty())
        return Status(false, "Error: RowSet is empty - dbms programmer needs to fix this");

    RowSet* rs = s.Tuples().at(0);
    if (rs->rows_.size() != 1)
        return Status(false, "Error: Subquery must produce a single row");

    if (rs->rows_.at(0)->data_.size() != 1)
        return Status(false, "Error: Subquery row must contain a single column");

    *result = rs->rows_.at(0)->data_.at(0);

    return Status();
}


Status Executor::BeginScan(WorkTable* scan) {
    switch (scan->Type()) {
        case ScanType::Left:
            return BeginScanLeft((LeftJoin*)scan);
        case ScanType::Full:
            return BeginScanFull((FullJoin*)scan);
        case ScanType::Inner:
            return BeginScanInner((InnerJoin*)scan);
        case ScanType::Cross:
            return BeginScanCross((CrossJoin*)scan);
        case ScanType::Constant:
            return BeginScanConstant((ConstantTable*)scan);
        case ScanType::Table:
            return BeginScanTable((PrimaryTable*)scan);
        default:
            return Status(false, "Execution Error: Invalid scan type");
    }
}

Status Executor::BeginScanLeft(LeftJoin* scan) {
    {
        Status s = BeginScan(scan->left_);
        //should we return if there's an error?
    }

    { 
        Status s = BeginScan(scan->right_);
        //should we return if there's an error?
    }

    //initialize left row
    Status s = NextRow(scan->left_, &scan->left_row_);
    scan->lefts_inserted_ = 0;
    if (!s.Ok())
        return Status(false, "No more rows");

    return Status();
}

Status Executor::BeginScanFull(FullJoin* scan) {
    BeginScan(scan->right_join_);
    scan->do_right_ = true;

    return Status();
}

Status Executor::BeginScanInner(InnerJoin* scan) {
    BeginScan(scan->left_);
    BeginScan(scan->right_);

    //initialize left row
    Status s = NextRow(scan->left_, &scan->left_row_);
    if (!s.Ok())
        return Status(false, "No more rows");

    return Status();
}

Status Executor::BeginScanCross(CrossJoin* scan) {
    BeginScan(scan->left_);
    BeginScan(scan->right_);

    //initialize left row
    Status s = NextRow(scan->left_, &scan->left_row_);
    if (!s.Ok())
        return Status(false, "No more rows");

    return Status();
}

Status Executor::BeginScanConstant(ConstantTable* scan) {
    scan->cur_ = 0;
    return Status();
}

Status Executor::BeginScanTable(PrimaryTable* scan) {
    return scan->table_->BeginScan(storage_, qs_.scan_idx);
}

Status Executor::NextRow(WorkTable* scan, Row** row) {
    switch (scan->Type()) {
        case ScanType::Left:
            return NextRowLeft((LeftJoin*)scan, row);
        case ScanType::Full:
            return NextRowFull((FullJoin*)scan, row);
        case ScanType::Inner:
            return NextRowInner((InnerJoin*)scan, row);
        case ScanType::Cross:
            return NextRowCross((CrossJoin*)scan, row);
        case ScanType::Constant:
            return NextRowConstant((ConstantTable*)scan, row);
        case ScanType::Table:
            return NextRowTable((PrimaryTable*)scan, row);
        default:
            return Status(false, "Execution Error: Invalid scan type");
    }
}

Status Executor::NextRowLeft(LeftJoin* scan, Row** r) {
    Row* right_row;

    while (true) {
        Status s = NextRow(scan->right_, &right_row);
        if (!s.Ok()) {
            //get new left
            {
                if (scan->lefts_inserted_ == 0) {
                    std::vector<Datum> result = scan->left_row_->data_;
                    for (int i = 0; i < scan->right_attr_count_; i++) {
                        result.push_back(Datum());
                    }

                    *r = new Row(result);
                    scan->lefts_inserted_++;

                    return Status();
                }

                Status s = NextRow(scan->left_, &scan->left_row_);
                scan->lefts_inserted_ = 0;
                if (!s.Ok())
                    return Status(false, "No more rows");
            }

            {
                BeginScan(scan->right_);
                Status s = NextRow(scan->right_, &right_row);
                if (!s.Ok())
                    return Status(false, "No more rows");
            } 
        }

        {
            std::vector<Datum> result = scan->left_row_->data_;
            result.insert(result.end(), right_row->data_.begin(), right_row->data_.end());

            *r = new Row(result);
            Datum d;

            scopes_.push_back(*r);
            Status s = Eval(scan->condition_, *r, &d);
            scopes_.pop_back();

            if (d.AsBool()) {
                scan->lefts_inserted_++;
                return Status();
            }

        }
    }

    return Status(false, "Debug Error: Should never see this message");
}

Status Executor::NextRowFull(FullJoin* scan, Row** r) {
    //attempt grabbing row from right_join_
    //if none left, initialize left_ and right_, and start processing left join but only use values with nulled right sides
    if (scan->do_right_) {
        Status s = NextRow(scan->right_join_, r);
        if (s.Ok()) {
            return s;
        } else {
            //reset to begin augmented left outer join scan
            scan->do_right_ = false;
            BeginScan(scan->left_);
            BeginScan(scan->right_);
            Status s = NextRow(scan->left_, &scan->left_row_);
            scan->lefts_inserted_ = 0;
            if (!s.Ok())
                return Status(false, "No more rows");
        }
    }
    

    //right join is done, so grabbing left join rows where the right side has no match, and will be nulled
    Row* right_row;

    while (true) {
        Status s = NextRow(scan->right_, &right_row);
        if (!s.Ok()) {
            //get new left
            if (scan->lefts_inserted_ == 0) {
                std::vector<Datum> result;
                int right_attr_count = scan->attr_count_ - scan->left_row_->data_.size();
                for (int i = 0; i < right_attr_count; i++) {
                    result.push_back(Datum());
                }

                result.insert(result.end(), scan->left_row_->data_.begin(), scan->left_row_->data_.end());

                *r = new Row(result);
                scan->lefts_inserted_++;

                return Status();
            }

            {
                Status s = NextRow(scan->left_, &scan->left_row_);
                scan->lefts_inserted_ = 0;
                if (!s.Ok())
                    return Status(false, "No more rows");
            }

            {
                BeginScan(scan->right_);
                Status s = NextRow(scan->right_, &right_row);
                if (!s.Ok())
                    return Status(false, "No more rows");
            } 
        }

        //checking 'on' condition, but not adding row if true since
        //that was already taken care of with the right outer join
        //only checking if condition is met or not so that we know
        //whether the current left row needs to be concatenated with a nulled right row 
        {
            std::vector<Datum> result = right_row->data_;
            result.insert(result.end(), scan->left_row_->data_.begin(), scan->left_row_->data_.end());

            Row temp(result);
            Datum d;

            scopes_.push_back(&temp);
            Status s = Eval(scan->condition_, &temp, &d);
            scopes_.pop_back();

            //TODO: optimization opportunity here
            //if a left join row has a matching right side here, no need to check the
            //rest of the right rows to determine if a left + nulled-right is necessary
            //can go straight to the next left row immediately
            if (d.AsBool()) {
                scan->lefts_inserted_++;
            }

        }
    }

    return Status(false, "Debug Errpr: Should never see this message");
}

Status Executor::NextRowInner(InnerJoin* scan, Row** r) {
    Row* right_row;

    while (true) {
        Status s = NextRow(scan->right_, &right_row);
        if (!s.Ok()) {
            //get new left
            {
                Status s = NextRow(scan->left_, &scan->left_row_);
                if (!s.Ok())
                    return Status(false, "No more rows");
            }

            {
                BeginScan(scan->right_);
                Status s = NextRow(scan->right_, &right_row);
                if (!s.Ok())
                    return Status(false, "No more rows");
            } 
        }

        //return concatenated row if condition is met - otherwise continue loop
        {
            std::vector<Datum> result = scan->left_row_->data_;
            result.insert(result.end(), right_row->data_.begin(), right_row->data_.end());

            *r = new Row(result);
            Datum d;

            //this is ugly, we need to do this since Expr::Eval is needed to generate a working table
            //something similar is done in InnerJoin::Analyze
            scopes_.push_back(*r);
            Status s = Eval(scan->condition_, *r, &d);
            scopes_.pop_back();

            if (d.AsBool())
                return Status();

        }
    }

    return Status(false, "Debug Error: Should never see this message");
}

Status Executor::NextRowCross(CrossJoin* scan, Row** r) {
    Row* right_row;
    Status s = NextRow(scan->right_, &right_row);
    if (!s.Ok()) {
        {
            Status s = NextRow(scan->left_, &scan->left_row_);
            if (!s.Ok())
                return Status(false, "No more rows");
        }

        {
            BeginScan(scan->right_);
            Status s = NextRow(scan->right_, &right_row);
            if (!s.Ok())
                return Status(false, "No more rows");
        } 
    }

    std::vector<Datum> result = scan->left_row_->data_;
    result.insert(result.end(), right_row->data_.begin(), right_row->data_.end());
    *r = new Row(result);

    return Status();
}

Status Executor::NextRowConstant(ConstantTable* scan, Row** r) {
    if (scan->cur_ > 0)
        return Status(false, "No more rows");

    std::vector<Datum> data;
    for (size_t i = 0; i < scan->target_cols_.size(); i++) {
        data.emplace_back(0);
    }
    *r = new Row(data);
    scan->cur_++;

    return Status();
}

Status Executor::NextRowTable(PrimaryTable* scan, Row** r) {
    return scan->table_->NextRow(storage_, r);
}

}

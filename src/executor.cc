#include <fstream>
#include <sstream>
#include <iostream> //TODO: remove this later

#include "executor.h"
#include "schema.h"
#include "tokenizer.h"
#include "parser.h"
#include "analyzer.h"

namespace wsldb {

std::vector<Status> Executor::ExecuteQuery(const std::string& query) {
    std::vector<Token> tokens = std::vector<Token>();
    {
        Tokenizer tokenizer(query);
        do {
            Token t;
            Status s = tokenizer.NextToken(&t);
            if (!s.Ok())
                return {s};

            tokens.push_back(t);
        } while (tokens.back().type != TokenType::Eof);
    }

    std::vector<Stmt*> stmts;
    {
        Parser parser(tokens);
        Status s = parser.ParseStmts(stmts);
        if (!s.Ok())
            return {s};
    }

    std::vector<Status> statuses;
    {
        Analyzer a(txn_);
        for (Stmt* stmt: stmts) {

            //creating a transaction if not explicitly created
            bool auto_commit = false;
            if ((*txn_) == nullptr && stmt->Type() != StmtType::TxnControl) {
                *txn_ = storage_->BeginTxn();
                auto_commit = true;
            }
            std::vector<DatumType> types;
            Status s = a.Verify(stmt, types);
            if (s.Ok())
                s = Execute(stmt);

            //ending automatically created txn
            if (auto_commit) {
                if ((*txn_)->has_aborted_) {
                    (*txn_)->Rollback();
                } else {
                    (*txn_)->Commit();
                }

                delete *txn_;
                *txn_ = nullptr;
            }

            statuses.push_back(s);
        }

    }

    return statuses;
}

Status Executor::Execute(Stmt* stmt) {

    if (*txn_ && (*txn_)->has_aborted_ && stmt->Type() != StmtType::TxnControl) {
        return Status(false, "Execution Error: Transaction has aborted and will ignore all statements until ended");
    }

    Status s;

    switch (stmt->Type()) {
        case StmtType::Create:
            s = CreateExecutor((CreateStmt*)stmt);
            break;
        case StmtType::Insert:
            s = InsertExecutor((InsertStmt*)stmt);
            break;
        case StmtType::Update:
            s = UpdateExecutor((UpdateStmt*)stmt);
            break;
        case StmtType::Delete:
            s = DeleteExecutor((DeleteStmt*)stmt);
            break;
        case StmtType::Select:
            s = SelectExecutor((SelectStmt*)stmt);
            break;
        case StmtType::DescribeTable:
            s = DescribeTableExecutor((DescribeTableStmt*)stmt);
            break;
        case StmtType::DropTable:
            s = DropTableExecutor((DropTableStmt*)stmt);
            break;
        case StmtType::TxnControl:
            s = TxnControlExecutor((TxnControlStmt*)stmt);
            break;
        case StmtType::CreateModel:
            s = CreateModelExecutor((CreateModelStmt*)stmt);
            break;
        case StmtType::DropModel:
            s = DropModelExecutor((DropModelStmt*)stmt);
            break;
        default:
            s = Status(false, "Execution Error: Invalid statement type");
            break;
    }

    if (*txn_ && !s.Ok())
        (*txn_)->has_aborted_ = true;

    return s;
}

Status Executor::Eval(Expr* expr, Datum* result) {
    switch (expr->Type()) {
        case ExprType::Literal:
            return Eval((Literal*)expr, result);
        case ExprType::Binary:
            return Eval((Binary*)expr, result);
        case ExprType::Unary:
            return Eval((Unary*)expr, result);
        case ExprType::ColRef:
            return Eval((ColRef*)expr, result);
        case ExprType::ColAssign:
            return Eval((ColAssign*)expr, result);
        case ExprType::Call:
            return Eval((Call*)expr, result);
        case ExprType::IsNull:
            return Eval((IsNull*)expr, result);
        case ExprType::ScalarSubquery:
            return Eval((ScalarSubquery*)expr, result);
        case ExprType::Predict:
            return Eval((Predict*)expr, result);
        case ExprType::Cast:
            return Eval((Cast*)expr, result);
        default:
            return Status(false, "Execution Error: Invalid expression type");
    }
}

Status Executor::PushEvalPop(Expr* expr, Row* row, Datum* result) {
    scopes_.push_back(row);
    //attrs_.push_back(attrs);

    Status s = Eval(expr, result);

    scopes_.pop_back();
    //attrs_.pop_back();

    return s;
}

Status Executor::CreateExecutor(CreateStmt* stmt) { 
    Schema schema(stmt->target_.lexeme, stmt->names_, stmt->types_, stmt->not_null_constraints_, stmt->uniques_);
    storage_->CreateTable(&schema, *txn_); //TODO: should use a txn to serialize schema here rather than using specialized function in Storage class

    return Status(); 
}


Status Executor::InsertExecutor(InsertStmt* stmt) { 
    for (std::vector<Expr*> exprs: stmt->col_assigns_) {
        Status s = InsertRow(stmt->scan_, exprs);
        if (!s.Ok()) return s;

    }

    return Status(); 
}

Status Executor::UpdateExecutor(UpdateStmt* stmt) { 
    BeginScan(stmt->scan_);

    Row* r;
    int update_count = 0;
    while (NextRow(stmt->scan_, &r).Ok()) {

        Row updated_row = *r;

        for (Expr* e: stmt->assigns_) {
            Datum d;
            attrs_.push_back(stmt->scan_->attrs_);
            Status s = PushEvalPop(e, &updated_row, &d); //returned Datum of ColAssign expressions are ignored
            attrs_.pop_back();

            if (!s.Ok()) 
                return s;
        }

        Status s = UpdateRow(stmt->scan_, r, &updated_row);
        if (!s.Ok()) return s;

        update_count++;
    }

    return Status(); 
}

Status Executor::DeleteExecutor(DeleteStmt* stmt) {
    BeginScan(stmt->scan_);

    Row* r;
    int delete_count = 0;
    while (NextRow(stmt->scan_, &r).Ok()) {
        Status s = DeleteRow(stmt->scan_, r);
        if (!s.Ok()) return s;
        delete_count++;
    }
    return Status(); 
}

Status Executor::SelectExecutor(SelectStmt* stmt) {
    Status s = BeginScan(stmt->scan_);
    RowSet* final_rs = new RowSet(((ProjectScan*)(stmt->scan_))->attrs_);
    Row* r;
    while (NextRow(stmt->scan_, &r).Ok()) {
        final_rs->rows_.push_back(r);
    }

    return Status(true, "(" + std::to_string(final_rs->rows_.size()) + " rows)", {final_rs});
}

Status Executor::DescribeTableExecutor(DescribeTableStmt* stmt) { 
    //column information
    std::vector<Attribute> row_description = { Attribute("rel_ref", "name", DatumType::Text, true), 
                                               Attribute("rel_ref", "type", DatumType::Text, true), 
                                               Attribute("rel_ref", "not null", DatumType::Bool, true) };
    RowSet* rowset = new RowSet(row_description);

    for (const Attribute& a: stmt->schema_->attrs_) {
        std::vector<Datum> data = { Datum(a.name), Datum(Datum::TypeToString(a.type)) };
        data.emplace_back(a.not_null_constraint);
        rowset->rows_.push_back(new Row(data));
    }

    //index information
    std::vector<Attribute> idx_row_description = { Attribute("rel_ref", "type", DatumType::Text, true),
                                                   Attribute("rel_ref", "name", DatumType::Text, true) };

    RowSet* idx_rowset = new RowSet(idx_row_description);

    std::string type = "lsm tree";
    for (const Index& i: stmt->schema_->idxs_) {
        std::vector<Datum> index_info = { Datum(type), Datum(i.name_) };
        idx_rowset->rows_.push_back(new Row(index_info));
    }

    return Status(true, "table '" + stmt->target_relation_.lexeme + "'", { rowset, idx_rowset });
}

Status Executor::DropTableExecutor(DropTableStmt* stmt) { 
    //drop table name from catalogue
    if (!stmt->schema_) {
        return Status(true, "(table '" + stmt->target_relation_.lexeme + "' doesn't exist and not dropped)");
    }

    storage_->DropTable(stmt->schema_, *txn_);

    return Status(true, "(table '" + stmt->target_relation_.lexeme + "' dropped)");
}

//TODO: should move this all to main Executor method so that all txn stuff can be kept together
Status Executor::TxnControlExecutor(TxnControlStmt* stmt) {
    switch (stmt->t_.type) {
        case TokenType::Begin: {
            *txn_ = storage_->BeginTxn();
            return Status();
        }
        case TokenType::Commit: {
            if ((*txn_)->has_aborted_) {
                (*txn_)->Rollback();
            } else {
                (*txn_)->Commit();
            }
            delete *txn_;
            *txn_ = nullptr;
            return Status();
        }
        case TokenType::Rollback: {
            (*txn_)->Rollback();
            delete *txn_;
            *txn_ = nullptr;
            return Status();
        }
        default:
            return Status(false, "Execution Error: Invalid token");
    }
}

Status Executor::CreateModelExecutor(CreateModelStmt* stmt) {
    std::string serialized_model;
    {
        std::ifstream in(inference_->CreateFullModelPath(stmt->path_.lexeme));
        std::stringstream buffer;
        if (buffer << in.rdbuf()) {
            serialized_model = buffer.str();
        } else {
            return Status(false, "Execution Error: Invalid path for model");
        }
    }

    return (*txn_)->Put(Storage::Models(), stmt->name_.lexeme, serialized_model);
}

//Analyzer should have returned error if table doesn't exist and 'if exists' not used
//drop model if it exists, otherwise just ignore error
Status Executor::DropModelExecutor(DropModelStmt* stmt) {
    std::string serialized_model;
    Status s = (*txn_)->Get(Storage::Models(), stmt->name_.lexeme, &serialized_model);

    if (s.Ok()) {
        return (*txn_)->Delete(Storage::Models(), stmt->name_.lexeme);
    }

    return Status();
}

/*
 * Expression Evaluators
 */

Status Executor::Eval(Literal* expr, Datum* result) {
    *result = Datum(LiteralTokenToDatumType(expr->t_.type), expr->t_.lexeme);
    return Status();
}

Status Executor::Eval(Binary* expr, Datum* result) { 
    Datum l;
    {
        Status s = Eval(expr->left_, &l);
        if (!s.Ok()) return s;
    }

    Datum r;
    {
        Status s = Eval(expr->right_, &r);
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

Status Executor::Eval(Unary* expr, Datum* result) {
    Datum right;
    Status s = Eval(expr->right_, &right);
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

Status Executor::Eval(ColRef* expr, Datum* result) {
    *result = scopes_.rbegin()[expr->scope_]->data_.at(expr->idx_);
    return Status();
}

Status Executor::Eval(ColAssign* expr, Datum* result) {
    Datum right;
    {
        Status s = Eval(expr->right_, &right);
        if (!s.Ok()) return s;
    }

    size_t i;
    int data_idx;
    {
        std::string table_ref = "";
        for (i = 0; i < scopes_.size(); i++) {
            AttributeSet* as = attrs_.rbegin()[i];
            Attribute a;
            Status s = as->GetAttribute(table_ref, expr->col_.lexeme, &a, &data_idx);
            if (s.Ok())
                break;
        }

        if (i >= scopes_.size()) {
            return Status(false, "Error: Column '" + table_ref + "." + expr->col_.lexeme + "' does not exist");
        }
    }

    if (right.Type() != DatumType::Null && expr->field_type_ != right.Type()) {
        Datum casted_datum;
        if (!Datum::Cast(right, expr->field_type_, &casted_datum))
            return Status(false, "Execution Error: Invalid cast");

        scopes_.rbegin()[i]->data_.at(data_idx) = casted_datum;
    } else {
        scopes_.rbegin()[i]->data_.at(data_idx) = right;
    }

    *result = right; //result not used
    return Status();
}

Status Executor::Eval(Call* expr, Datum* result) {
    is_agg_ = true;
    Datum arg;
    Status s = Eval(expr->arg_, &arg);
    if (!s.Ok())
        return s;

    switch (expr->fcn_.type) {
        case TokenType::Avg:
            expr->sum_ += arg;
            expr->count_ += Datum(static_cast<int64_t>(1));
            *result = expr->sum_ / expr->count_;
            break;
        case TokenType::Count: {
            expr->count_ += Datum(static_cast<int64_t>(1));
            *result = expr->count_;
            break;
        }
        case TokenType::Max:
            if (expr->first_ || arg > expr->max_) {
                expr->max_ = arg;
                expr->first_ = false;
            }
            *result = expr->max_;
            break;
        case TokenType::Min:
            if (expr->first_ || arg < expr->min_) {
                expr->min_ = arg;
                expr->first_ = false;
            }
            *result = expr->min_;
            break;
        case TokenType::Sum:
            expr->sum_ += arg;
            *result = expr->sum_;
            break;
        default:
            return Status(false, "Error: Invalid function name");
    }

    //Calling Eval on argument may set qs.is_agg to false, so need to set it after evaluating argument

    return Status();
}

Status Executor::Eval(IsNull* expr, Datum* result) {
    Datum d;
    Status s = Eval(expr->left_, &d);
    if (!s.Ok())
        return s;

    if (d.IsType(DatumType::Null)) {
        *result = Datum(true);
    } else {
        *result = Datum(false);
    }

    return Status();
}

Status Executor::Eval(ScalarSubquery* expr, Datum* result) {
    bool old_is_agg = is_agg_;
    Status s = Execute(expr->stmt_);
    is_agg_ = old_is_agg;

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

Status Executor::Eval(Predict* expr, Datum* result) {
    Datum d;
    {
        Status s = Eval(expr->arg_, &d);
        if (!s.Ok())
            return s;
    }

    std::string serialized_model;
    {
        Status s = (*txn_)->Get(Storage::Models(), expr->model_name_.lexeme, &serialized_model);
        if (!s.Ok())
            return Status(false, "Analysis Error: Model with the name '" + expr->model_name_.lexeme + "' does not exist");
    }

    Model* model;
    {
        Status s = inference_->DeserializeModel(serialized_model, &model);
        if (!s.Ok())
            return s;
    }

    std::vector<int> results;
    Status s = model->Predict(d.Data(), results);
    if (!s.Ok())
        return s;

    *result = results.at(0); //just getting first result for now
   
    return Status();
}

Status Executor::Eval(Cast* expr, Datum* result) {
    Datum d;
    {
        Status s = Eval(expr->value_, &d);
        if (!s.Ok())
            return s;
    }

    if (!Datum::Cast(d, TypeTokenToDatumType(expr->type_.type), result)) {
        return Status(false, "Execution Error: Casting of value failed");
    }

    return Status();
}

/*
 * Scans
 */

Status Executor::BeginScan(Scan* scan) {
    switch (scan->Type()) {
        case ScanType::Constant:
            return BeginScanConstant((ConstantScan*)scan);
        case ScanType::Table:
            return BeginScanTable((TableScan*)scan);
        case ScanType::Select:
            return BeginScan((SelectScan*)scan);
        case ScanType::Product:
            return BeginScan((ProductScan*)scan);
        case ScanType::OuterSelect:
            return BeginScan((OuterSelectScan*)scan);
        case ScanType::Project:
            return BeginScan((ProjectScan*)scan);
        default:
            return Status(false, "Execution Error: Invalid scan type");
    }
}

Status Executor::BeginScanConstant(ConstantScan* scan) {
    scan->cur_ = 0;
    return Status();
}

Status Executor::BeginScanTable(TableScan* scan) {
    /*
    int scan_idx = 0;
    return scan->table_->BeginScan(storage_, scan_idx);*/
    scan->scan_idx_ = 0; //TODO: should change this if using index scan
    scan->it_ = storage_->NewIterator(scan->schema_->idxs_.at(scan->scan_idx_).name_);
    scan->it_->SeekToFirst();

    return Status();
}

Status Executor::BeginScan(SelectScan* scan) {
    Status s = BeginScan(scan->scan_);
    return s;
}

Status Executor::BeginScan(ProductScan* scan) {
    {
        Status s = BeginScan(scan->left_);
        if (!s.Ok()) return s;
    }

    {
        Status s = BeginScan(scan->right_);
        if (!s.Ok()) return s;
    }

    return Status();
}

Status Executor::BeginScan(OuterSelectScan* scan) {
    {
        Status s = BeginScan(scan->scan_);
        if (!s.Ok()) return s;
    }

    scan->scanning_rows_ = true;

    return Status();
}

Status Executor::BeginScan(ProjectScan* scan) {
    scan->cursor_ = 0;
    RowSet* rs = new RowSet(scan->attrs_);
    {
        Status s = BeginScan(scan->input_);
        if (!s.Ok()) return s;

        Row* r;
        while (NextRow(scan->input_, &r).Ok()) {
            rs->rows_.push_back(r);
        }
    }

    //sort filtered rows in-place
    //TODO: should sort while populating output_ rather than sorting after the fact
    if (!scan->order_cols_.empty()) {
        //lambdas can only capture non-member variables
        std::vector<OrderCol>& order_cols = scan->order_cols_;

        std::sort(rs->rows_.begin(), rs->rows_.end(), 
                //No error checking in lambda...
                //do scope rows need to be pushed/popped of query state stack here???
                [order_cols, this](Row* t1, Row* t2) -> bool { 
                for (OrderCol oc: order_cols) {
                    Datum d1;
                    this->PushEvalPop(oc.col, t1, &d1);

                    Datum d2;
                    this->PushEvalPop(oc.col, t2, &d2);

                    if (d1 == d2)
                        continue;

                    Row* r = nullptr;
                    Datum d;
                    this->PushEvalPop(oc.asc, r, &d);
                    if (d.AsBool()) {
                        return d1 < d2;
                    }
                    return d1 > d2;
                }

                return true;
                });
    }

    //TODO: rewrite this to compute aggregates row-by-row
    //Then integrate into the beginning of this function when rows are first read
    //will need to make phantom rows for sorting
    //Make sure tests in agg_inside_expression.sql pass (will use that to make sure above refactor doesn't break anything)


    //projection
    RowSet* proj_rs = new RowSet(scan->attrs_);

    std::vector<Datum> data;
    for (Expr* e: scan->projs_) {
        e->Reset();
    }
    bool row_has_agg = false;

    for (Row* r: rs->rows_) {
        row_has_agg = false;
        data.clear();
        int idx = 0;

        for (Expr* e: scan->projs_) {
            is_agg_ = false;
            Datum d;
            Status s = PushEvalPop(e, r, &d);

            if (!s.Ok()) return s;

            if (is_agg_) {
                Attribute a = scan->attrs_.at(idx);
                scan->attrs_.at(idx) = Attribute(a.rel_ref, a.name, d.Type(), a.not_null_constraint);
            }

            data.push_back(d);
            idx++;

            if (is_agg_)
                row_has_agg = true;
        }

        if (!row_has_agg) {
            proj_rs->rows_.push_back(new Row(data));
        }
    }

    if (row_has_agg) {
        proj_rs->rows_.push_back(new Row(data));
    }




    //remove duplicates
    scan->output_ = new RowSet(scan->attrs_);

    if (scan->distinct_) {
        std::unordered_map<std::string, bool> map;
        for (Row* r: proj_rs->rows_) {
            std::string key = Datum::SerializeData(r->data_);
            if (map.find(key) == map.end()) {
                map.insert({key, true});
                scan->output_->rows_.push_back(r);
            }
        }
    } else {
        scan->output_->rows_ = proj_rs->rows_;
    }

    //limit in-place
    Row dummy_row({});
    Datum d;
    //do rows need to be pushed/popped on query state row stack here?
    //should scalar subqueries be allowed in the limit clause?
    Status s = PushEvalPop(scan->limit_, &dummy_row, &d);
    if (!s.Ok()) return s;

    size_t limit = d == -1 ? std::numeric_limits<size_t>::max() : d.AsInt8();
    if (limit < scan->output_->rows_.size()) {
        scan->output_->rows_.resize(limit);
    }

    return Status();
}

Status Executor::NextRow(Scan* scan, Row** row) {
    switch (scan->Type()) {
        case ScanType::Constant:
            return NextRowConstant((ConstantScan*)scan, row);
        case ScanType::Table:
            return NextRowTable((TableScan*)scan, row);
        case ScanType::Select:
            return NextRow((SelectScan*)scan, row);
        case ScanType::Product:
            return NextRow((ProductScan*)scan, row);
        case ScanType::OuterSelect:
            return NextRow((OuterSelectScan*)scan, row);
        case ScanType::Project:
            return NextRow((ProjectScan*)scan, row);
        default:
            return Status(false, "Execution Error: Invalid scan type");
    }
}

Status Executor::NextRowConstant(ConstantScan* scan, Row** r) {
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

Status Executor::NextRowTable(TableScan* scan, Row** r) {
    if (!scan->it_->Valid()) return Status(false, "no more records");

    std::string value = scan->it_->Value();
  
    //if scan is using secondary index, value is primary key
    //return record stored in primary index
    int primary_cf = 0; 
    if (scan->scan_idx_ != primary_cf) {
        //TableHandle handle = storage_->GetTable(scan->tab_name_);
        std::string primary_key = value;
        //handle.db->Get(rocksdb::ReadOptions(), handle.cfs.at(primary_cf), primary_key, &value);

        (*txn_)->Get(scan->schema_->idxs_.at(0).name_, primary_key, &value);
    }

    *r = new Row(scan->schema_->DeserializeData(value));
    scan->it_->Next();

    return Status();
}

Status Executor::NextRow(SelectScan* scan, Row** r) {
    while (true) {
        {
            Status s = NextRow(scan->scan_, r);
            if (!s.Ok())
                return s;
        }

        Datum result;
        {
            Status s = PushEvalPop(scan->expr_, *r, &result);

            if (!s.Ok())
                return s;
        }

        if (result.AsBool()) {
            return Status();
        }
    }

    return Status(false, "No more records");
}

Status Executor::NextRow(ProductScan* scan, Row** r) {
    if (!scan->left_row_) {
        Status s = NextRow(scan->left_, &scan->left_row_);
        if (!s.Ok())
            return Status(false, "No more records");
    }

    Row* right_row;
    {
        Status s = NextRow(scan->right_, &right_row);
        if (!s.Ok()) {
            {
                Status s2 = BeginScan(scan->right_);
                if (!s2.Ok())
                    return s;
            }
            {
                Status s2 = NextRow(scan->left_, &scan->left_row_);
                if (!s2.Ok())
                    return Status(false, "No more records");
            }
            {
                Status s2 = NextRow(scan->right_, &right_row);
                if (!s2.Ok())
                    return Status(false, "No more records");
            }
        }
    }    


    std::vector<Datum> result = scan->left_row_->data_;
    result.insert(result.end(), right_row->data_.begin(), right_row->data_.end());
    *r = new Row(result);

    return Status();
}


Status Executor::NextRow(OuterSelectScan* scan, Row** r) {
    while (NextRow(scan->scan_, r).Ok()) {
        std::string left_key;
        std::string right_key;
        for (size_t i = 0; i < (*r)->data_.size(); i++) {
            Datum& d = (*r)->data_.at(i);
            if (i < scan->scan_->left_->attrs_->AttributeCount()) {
                left_key += d.Serialize();
            } else {
                right_key += d.Serialize();
            } 
        }

        if (scan->left_pass_table_.find(left_key) == scan->left_pass_table_.end()) {
            scan->left_pass_table_.insert({left_key, false});
        }

        if (scan->right_pass_table_.find(right_key) == scan->right_pass_table_.end()) {
            scan->right_pass_table_.insert({right_key, false});
        }

        Datum result;
        {
            Status s = PushEvalPop(scan->expr_, *r, &result);

            if (!s.Ok())
                return s;
        }

        if (result.AsBool()) {
            scan->left_pass_table_.at(left_key) = true;
            scan->right_pass_table_.at(right_key) = true;
            return Status();
        }
    }

    if (scan->scanning_rows_) {
        scan->scanning_rows_ = false;
        scan->left_it_ = scan->include_left_ ? scan->left_pass_table_.begin() : scan->left_pass_table_.end();
        scan->right_it_ = scan->include_right_ ? scan->right_pass_table_.begin() : scan->right_pass_table_.end();
    }

   while (scan->left_it_ != scan->left_pass_table_.end()) {
        if (!scan->left_it_->second) {
            int off = 0;
            std::vector<Datum> data;
            for (const Attribute& a: scan->scan_->left_->attrs_->GetAttributes()) {
                Datum d(scan->left_it_->first, &off, a.type);
                data.push_back(d); 
            }

            for (size_t i = 0; i < scan->scan_->right_->attrs_->AttributeCount(); i++) {
                data.emplace_back();
            }

            *r = new Row(data);
            scan->left_it_++;

            return Status();
        }

        scan->left_it_++;
    }

    while (scan->right_it_ != scan->right_pass_table_.end()) {
        if (!scan->right_it_->second) {
            std::vector<Datum> data;
            for (size_t i = 0; i < scan->scan_->left_->attrs_->AttributeCount(); i++) {
                data.emplace_back();
            }

            int off = 0;
            for (const Attribute& a: scan->scan_->right_->attrs_->GetAttributes()) {
                Datum d(scan->right_it_->first, &off, a.type);
                data.push_back(d); 
            }

            *r = new Row(data);
            scan->right_it_++;

            return Status();
        }

        scan->right_it_++;
    }

    return Status(false, "No more records");
}

Status Executor::NextRow(ProjectScan* scan, Row** r) {
    if (scan->cursor_ < scan->output_->rows_.size()) {
        *r = scan->output_->rows_.at(scan->cursor_);
        scan->cursor_++;
        return Status();
    }

    return Status(false, "No more records");
}


Status Executor::DeleteRow(Scan* scan, Row* r) {
    switch (scan->Type()) {
        case ScanType::Table:
            return DeleteRow((TableScan*)scan, r);
        case ScanType::Select:
            return DeleteRow((SelectScan*)scan, r);
        default:
            return Status(false, "Execution Error: Only table scans and select scans allow record deletion");
    }
}

Status Executor::DeleteRow(SelectScan* scan, Row* r) {
    return DeleteRow(scan->scan_, r);
}

Status Executor::DeleteRow(TableScan* scan, Row* r) {
    //delete from primary index
    {
        Index* primary_idx = &scan->schema_->idxs_.at(0);
        (*txn_)->Delete(primary_idx->name_, primary_idx->GetKeyFromFields(r->data_));
    }

    //delete key/value in all secondary indexes 
    {
        for (size_t i = 1; i < scan->schema_->idxs_.size(); i++) {
            Index* secondary_idx = &scan->schema_->idxs_.at(i);
            (*txn_)->Delete(secondary_idx->name_, secondary_idx->GetKeyFromFields(r->data_));
        }
    }

    return Status();
}

Status Executor::UpdateRow(Scan* scan, Row* old_r, Row* new_r) {
    switch (scan->Type()) {
        case ScanType::Table:
            return UpdateRow((TableScan*)scan, old_r, new_r);
        case ScanType::Select:
            return UpdateRow((SelectScan*)scan, old_r, new_r);
        default:
            return Status(false, "Execution Error: Only table scans and select scans allow record updates");
    }
}

Status Executor::UpdateRow(SelectScan* scan, Row* old_r, Row* new_r) {
    return UpdateRow(scan->scan_, old_r, new_r);
}

Status Executor::UpdateRow(TableScan* scan, Row* old_r, Row* new_r) {
    //update primary index
    std::string updated_primary_key;
    {
        Index* primary_idx = &scan->schema_->idxs_.at(0);
        std::string old_key = primary_idx->GetKeyFromFields(old_r->data_);
        updated_primary_key = primary_idx->GetKeyFromFields(new_r->data_);

        if (old_key.compare(updated_primary_key) != 0) {
            std::string dummy_value;
            if ((*txn_)->Get(primary_idx->name_, updated_primary_key, &dummy_value).Ok())
                return Status(false, "Error: A record with the same primary key already exists");

            (*txn_)->Delete(primary_idx->name_, old_key);
        }

        (*txn_)->Put(primary_idx->name_, updated_primary_key, Datum::SerializeData(new_r->data_));
    }

    //update secondary indexes
    {
        std::string dummy_value;
        for (size_t i = 1; i < scan->schema_->idxs_.size(); i++) {
            Index* secondary_idx = &scan->schema_->idxs_.at(i);
            std::string old_key = secondary_idx->GetKeyFromFields(old_r->data_);
            std::string updated_key = secondary_idx->GetKeyFromFields(new_r->data_);

            if (old_key.compare(updated_key) != 0) {
                std::string tmp_value;
                if ((*txn_)->Get(secondary_idx->name_, updated_key, &dummy_value).Ok())
                    return Status(false, "Error: A record with the same secondary key already exists");

                (*txn_)->Delete(secondary_idx->name_, old_key);
            }

            (*txn_)->Put(secondary_idx->name_, updated_key, updated_primary_key);
        }

    }

    return Status();
}

Status Executor::InsertRow(Scan* scan, const std::vector<Expr*>& exprs) {
    switch (scan->Type()) {
        case ScanType::Table:
            return InsertRow((TableScan*)scan, exprs);
        default:
            return Status(false, "Execution Error: Only table scans allow record insertion");
    }
}

Status Executor::InsertRow(TableScan* scan, const std::vector<Expr*>& exprs) {
    //fill call fields with default null
    std::vector<Datum> nulls;
    for (size_t i = 0; i < scan->schema_->attrs_.size(); i++) {
        nulls.push_back(Datum());
    }

    Row r(nulls);

    for (Expr* e: exprs) {
        Datum d;
        attrs_.push_back(scan->attrs_);
        Status s = PushEvalPop(e, &r, &d); //result d is not used
        attrs_.pop_back();
        if (!s.Ok()) return s;
    }

    //insert autoincrementing _rowid
    {
        //insertions will leave space at first index for _rowid
        //TODO: this will NOT work if multiple txns try to use RowId at the same time - lost update problem here
        //When schema is read, need to use GetForUpdate to prevent lost updates
        int64_t rowid = scan->schema_->NextRowId();
        r.data_.at(0) = Datum(rowid);
    }

    //insert into primary index
    std::string primary_key;
    {

        Index* primary_idx = &scan->schema_->idxs_.at(0);
        std::string value = Datum::SerializeData(r.data_);
        primary_key = primary_idx->GetKeyFromFields(r.data_);
        std::string test_value;
        if ((*txn_)->Get(primary_idx->name_, primary_key, &test_value).Ok())
            return Status(false, "Error: A record with the same primary key already exists");

        (*txn_)->Put(primary_idx->name_, primary_key, value);
    }

    //insert into secondary indexes
    {
        std::string test_value;
        for (size_t i = 1; i < scan->schema_->idxs_.size(); i++) {
            Index* idx = &scan->schema_->idxs_.at(i);
            std::string secondary_key = idx->GetKeyFromFields(r.data_);
            if ((*txn_)->Get(idx->name_, secondary_key, &test_value).Ok())
                return Status(false, "Error: A record with the same secondary key already exists");

            (*txn_)->Put(idx->name_, secondary_key, primary_key);
        }

        (*txn_)->Put(Storage::Catalog(), scan->schema_->table_name_, scan->schema_->Serialize());
    }

    return Status();
}

}

#include "analyzer.h"
#include "table.h"

namespace wsldb {

Status Analyzer::Verify(Stmt* stmt, AttributeSet** working_attrs) {
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
            return SelectVerifier((SelectStmt*)stmt, working_attrs);
        case StmtType::DescribeTable:
            return DescribeTableVerifier((DescribeTableStmt*)stmt);
        case StmtType::DropTable:
            return DropTableVerifier((DropTableStmt*)stmt);
        case StmtType::TxnControl:
            return TxnControlVerifier((TxnControlStmt*)stmt);
        case StmtType::CreateModel:
            return CreateModelVerifier((CreateModelStmt*)stmt);
        case StmtType::DropModel:
            return DropModelVerifier((DropModelStmt*)stmt);
        default:
            return Status(false, "Execution Error: Invalid statement type");
    }
}

Status Analyzer::CreateVerifier(CreateStmt* stmt) { 
    Table* schema;
    Status s = GetSchema(stmt->target_.lexeme, &schema);
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

Status Analyzer::InsertVerifier(InsertStmt* stmt) { 
    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->scan_, &working_attrs);
        if (!s.Ok()) return s;
    }

    if (!stmt->scan_->IsUpdatable()) {
        return Status(false, "Analysis Error: Cannot scan type is not updatable");
    }

    scopes_.push_back(working_attrs);
    for (const std::vector<Expr*>& assigns: stmt->col_assigns_) {
        for (Expr* e: assigns) {
            Attribute attr;
            Status s = Verify(e, &attr);
            if (!s.Ok())
                return s;
        }
    }
    scopes_.pop_back();

    return Status(); 
}


Status Analyzer::UpdateVerifier(UpdateStmt* stmt) { 
    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->scan_, &working_attrs);
        if (!s.Ok())
            return s;
    }

    scopes_.push_back(working_attrs);
    for (Expr* e: stmt->assigns_) {
        Attribute attr;
        Status s = Verify(e, &attr);
        if (!s.Ok())
            return s;
    }

    scopes_.pop_back();
    return Status(); 
}


Status Analyzer::DeleteVerifier(DeleteStmt* stmt) { 
    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->scan_, &working_attrs);
        if (!s.Ok())
            return s;
    }

    return Status(); 
}

Status Analyzer::SelectVerifier(SelectStmt* stmt, AttributeSet** working_attrs) {
    {
        Status s = Verify(stmt->scan_, working_attrs);
        if (!s.Ok()) return s;
    }

    return Status(); 
}


Status Analyzer::DescribeTableVerifier(DescribeTableStmt* stmt) { 
    Status s = GetSchema(stmt->target_relation_.lexeme, &stmt->schema_);
    if (!s.Ok())
        return s;

    return Status(); 
}


Status Analyzer::DropTableVerifier(DropTableStmt* stmt) { 
    Status s = GetSchema(stmt->target_relation_.lexeme, &stmt->schema_);
    if (!s.Ok() && !stmt->has_if_exists_)
        return s;

    return Status(); 
}

Status Analyzer::TxnControlVerifier(TxnControlStmt* stmt) {
    switch (stmt->t_.type) {
        case TokenType::Begin: {
            if (*txn_)
                return Status(false, "Analysis Error: Cannot use 'begin' when already inside a transaction");
            break;
        }
        case TokenType::Commit:
        case TokenType::Rollback: {
            if (!(*txn_))
                return Status(false, "Analysis Error: Cannot use 'commit'/'rollback' outside of a transaction");
            break;
        }
        default:
            return Status(false, "Analysis Error: Invalid token");
    }
    
    return Status();
}

Status Analyzer::CreateModelVerifier(CreateModelStmt* stmt) {
    std::string serialized_model;
    Status s = (*txn_)->Get(Storage::Models(), stmt->name_.lexeme, &serialized_model);
    if (s.Ok())
        return Status(false, "Analysis Error: Model with the name '" + stmt->name_.lexeme + "' already exists");

    return Status();
}

Status Analyzer::DropModelVerifier(DropModelStmt* stmt) {
    std::string serialized_model;
    Status s = (*txn_)->Get(Storage::Models(), stmt->name_.lexeme, &serialized_model);
    if (!s.Ok() && !stmt->has_if_exists_)
        return s;

    return Status(); 
}

/*
 * Expression Verifiers
 */

Status Analyzer::Verify(Expr* expr, Attribute* attr) {
    switch (expr->Type()) {
        case ExprType::Literal:
            return VerifyLiteral((Literal*)expr, attr);
        case ExprType::Binary:
            return VerifyBinary((Binary*)expr, attr);
        case ExprType::Unary:
            return VerifyUnary((Unary*)expr, attr);
        case ExprType::ColRef:
            return VerifyColRef((ColRef*)expr, attr);
        case ExprType::ColAssign:
            return VerifyColAssign((ColAssign*)expr, attr);
        case ExprType::Call:
            return VerifyCall((Call*)expr, attr);
        case ExprType::IsNull:
            return VerifyIsNull((IsNull*)expr, attr);
        case ExprType::ScalarSubquery:
            return VerifyScalarSubquery((ScalarSubquery*)expr, attr);
        case ExprType::Predict:
            return VerifyPredict((Predict*)expr, attr);
        case ExprType::Cast:
            return VerifyCast((Cast*)expr, attr);
        default:
            return Status(false, "Execution Error: Invalid expression type");
    }
}

Status Analyzer::VerifyLiteral(Literal* expr, Attribute* attr) { 
    *attr = Attribute("", "", LiteralTokenToDatumType(expr->t_.type));
    return Status(); 
}

Status Analyzer::VerifyBinary(Binary* expr, Attribute* attr) { 
    Attribute left_attr;
    {
        Status s = Verify(expr->left_, &left_attr);
        if (!s.Ok())
            return s;
    }
    Attribute right_attr;
    {
        Status s = Verify(expr->right_, &right_attr);
        if (!s.Ok())
            return s;
    }

    if (left_attr.type == DatumType::Null || right_attr.type == DatumType::Null) {
        *attr = Attribute("", expr->ToString(), DatumType::Null);
        return Status();
    }

    switch (expr->op_.type) {
        case TokenType::Equal:
        case TokenType::NotEqual:
        case TokenType::Less:
        case TokenType::LessEqual:
        case TokenType::Greater:
        case TokenType::GreaterEqual:
            if (!(Datum::TypeIsNumeric(left_attr.type) && Datum::TypeIsNumeric(right_attr.type)) && left_attr.type != right_attr.type) {
                    return Status(false, "Error: Equality and relational operands must be same data types");
            }
            *attr = Attribute("", expr->ToString(), DatumType::Bool);
            break;
        case TokenType::Or:
        case TokenType::And:
            if (!(left_attr.type == DatumType::Bool && right_attr.type == DatumType::Bool)) {
                return Status(false, "Error: Logical operator operands must be boolean types");
            }
            *attr = Attribute("", expr->ToString(), DatumType::Bool);
            break;
        case TokenType::Plus:
        case TokenType::Minus:
        case TokenType::Star:
        case TokenType::Slash:
            if (!(Datum::TypeIsNumeric(left_attr.type) && Datum::TypeIsNumeric(right_attr.type))) {
                return Status(false, "Error: The '" + expr->op_.lexeme + "' operator operands must both be a numeric type");
            } 
            *attr = Attribute("", expr->ToString(), left_attr.type);
            break;
        default:
            return Status(false, "Implementation Error: op type not implemented in Binary expr!");
            break;
    }

    return Status(); 
}

Status Analyzer::VerifyUnary(Unary* expr, Attribute* attr) { 
    Attribute right_attr;
    {
        Status s = Verify(expr->right_, &right_attr);
        if (!s.Ok())
            return s;
    }

    switch (expr->op_.type) {
        case TokenType::Not:
            if (right_attr.type != DatumType::Bool) {
                return Status(false, "Error: 'not' operand must be a boolean type.");
            }
            *attr = Attribute("", expr->ToString(), DatumType::Bool);
            break;
        case TokenType::Minus:
            if (!Datum::TypeIsNumeric(right_attr.type)) {
                return Status(false, "Error: '-' operator operand must be numeric type");
            }
            *attr = Attribute("", expr->ToString(), right_attr.type);
            break;
        default:
            return Status(false, "Implementation Error: op type not implemented in Binary expr!");
            break;
    }

    return Status(); 
}

Status Analyzer::VerifyColRef(ColRef* expr, Attribute* attr) { 
    {
        Status s;
        int dummy_idx;
        for (size_t i = 0; i < scopes_.size(); i++) {
            AttributeSet* as = scopes_.rbegin()[i];
            s = as->ResolveColumnTable(&expr->col_);
            if (!s.Ok()) return s;

            s = as->GetAttribute(&expr->col_, attr, &dummy_idx);
            if (s.Ok())
                break;
        }
        if (!s.Ok())
            return s;
    }

    return Status(); 
}

Status Analyzer::VerifyColAssign(ColAssign* expr, Attribute* attr) { 
    Attribute right_attr;
    {
        Status s = Verify(expr->right_, &right_attr);
        if (!s.Ok())
            return s;
    }

    {
        Status s;
        int dummy_idx;
        for (size_t i = 0; i < scopes_.size(); i++) {
            AttributeSet* as = scopes_.rbegin()[i];
            s = as->ResolveColumnTable(&expr->col_);
            if (!s.Ok()) return s;

            s = as->GetAttribute(&expr->col_, attr, &dummy_idx);
            if (s.Ok()) {
                s = as->PassesConstraintChecks(&expr->col_, right_attr.type);
                if (!s.Ok()) return s;

                break;
            }
        }
        if (!s.Ok())
            return s;

    }

    expr->field_type_ = attr->type;

    return Status(); 
}

Status Analyzer::VerifyCall(Call* expr, Attribute* attr) { 
    Attribute arg_attr;
    {
        Status s = Verify(expr->arg_, &arg_attr);
        if (!s.Ok()) {
            return s;
        }
    }

    switch (expr->fcn_.type) {
        case TokenType::Avg:
            //TODO: need to think about how we want to deal with integer division
            //using floor division now if argument is integer type - what does postgres do?
            *attr = Attribute("", expr->ToString(), arg_attr.type);
            break;
        case TokenType::Count:
            *attr = Attribute("", expr->ToString(), DatumType::Int8);
            break;
        case TokenType::Max:
        case TokenType::Min:
        case TokenType::Sum:
            *attr = Attribute("", expr->ToString(), arg_attr.type);
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

Status Analyzer::VerifyIsNull(IsNull* expr, Attribute* attr) { 
    *attr = Attribute("", expr->ToString(), DatumType::Bool);

    Attribute left_attr;
    Status s = Verify(expr->left_, &left_attr);
    if (!s.Ok())
        return s;

    return Status(); 
}

Status Analyzer::VerifyScalarSubquery(ScalarSubquery* expr, Attribute* attr) { 
    AttributeSet* working_attrs;
    {
        Status s = Verify(expr->stmt_, &working_attrs);
        if (!s.Ok())
            return s;
    }

    if (working_attrs->AttributeCount() != 1)
        return Status(false, "Error: Scalar subquery must return a single value");

    *attr = working_attrs->GetAttributes().at(0);

    return Status();
}

Status Analyzer::VerifyPredict(Predict* expr, Attribute* attr) {
    {
        Attribute arg_attr;
        Status s = Verify(expr->arg_, &arg_attr);
        if (!s.Ok())
            return s;

        *attr = Attribute("", expr->ToString(), DatumType::Int8); //temp to test mnist
    }

    {
        std::string serialized_model;
        Status s = (*txn_)->Get(Storage::Models(), expr->model_name_.lexeme, &serialized_model);
        if (!s.Ok())
            return Status(false, "Analysis Error: Model with the name '" + expr->model_name_.lexeme + "' does not exist");
    }

    return Status();
}

Status Analyzer::VerifyCast(Cast* expr, Attribute* attr) {
    Attribute value_attr;
    {
        Status s = Verify(expr->value_, &value_attr);
        if (!s.Ok())
            return s;
    }

    DatumType target_type = TypeTokenToDatumType(expr->type_.type);

    if (Datum::CanCast(value_attr.type, target_type)) {
        *attr = Attribute("", expr->ToString(), target_type);
        return Status();
    }

    return Status(false, "Analysis Error: Attempting to cast to an invalid type");
}

/*
 * Verify Scan
 */

Status Analyzer::Verify(Scan* scan, AttributeSet** working_attrs) {
    switch (scan->Type()) {
        case ScanType::Constant:
            return VerifyConstant((ConstantScan*)scan, working_attrs);
        case ScanType::Table:
            return VerifyTable((TableScan*)scan, working_attrs);
        case ScanType::Select:
            return VerifySelectScan((SelectScan*)scan, working_attrs);
        case ScanType::Product:
            return Verify((ProductScan*)scan, working_attrs);
        case ScanType::OuterSelect:
            return Verify((OuterSelectScan*)scan, working_attrs);
        case ScanType::Project:
            return Verify((ProjectScan*)scan, working_attrs);
        default:
            return Status(false, "Execution Error: Invalid scan type");
    }
}


Status Analyzer::VerifyConstant(ConstantScan* scan, AttributeSet** working_attrs) {
    std::vector<Attribute> attrs;
    std::vector<bool> dummy_not_nulls;
    for (Expr* e: scan->target_cols_) {
        Attribute attr;
        Status s = Verify(e, &attr);
        if (!s.Ok())
            return s;
        attrs.push_back(attr);
        dummy_not_nulls.push_back(false);
    }

    *working_attrs = new AttributeSet(attrs, dummy_not_nulls);
    scan->output_attrs_ = *working_attrs;

    return Status();
}

Status Analyzer::VerifyTable(TableScan* scan, AttributeSet** working_attrs) {
    bool ok = GetSchema(scan->tab_name_, &scan->table_).Ok();

    if (!ok) {
        return Status(false, "Error: Table '" + scan->tab_name_ + "' does not exist");
    }

    *working_attrs = scan->table_->MakeAttributeSet(scan->ref_name_);
    scan->output_attrs_ = *working_attrs;

    return Status();
}

Status Analyzer::VerifySelectScan(SelectScan* scan, AttributeSet** working_attrs) {
    {
        Status s = Verify(scan->scan_, working_attrs);
        if (!s.Ok())
           return s; 
    }

    scopes_.push_back(*working_attrs);

    {
        Attribute attr;
        Status s = Verify(scan->expr_, &attr);
        if (!s.Ok())
           return s; 

        if (attr.type != DatumType::Bool) {
            return Status(false, "Analysis Error: where clause expression must evaluate to true or false");
        }
    }
    scan->output_attrs_ = *working_attrs;

    scopes_.pop_back();

    return Status();
}

Status Analyzer::Verify(ProductScan* scan, AttributeSet** working_attrs) {
    AttributeSet* left_attrs;
    {
        Status s = Verify(scan->left_, &left_attrs);
        if (!s.Ok())
            return s;
    }
    AttributeSet* right_attrs;
    {
        Status s = Verify(scan->right_, &right_attrs);
        if (!s.Ok())
            return s;
    }
    {
        bool has_duplicate_tables;
        *working_attrs = new AttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
        scan->output_attrs_ = *working_attrs;
        if (has_duplicate_tables)
            return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
    }
    return Status();
}

Status Analyzer::Verify(OuterSelectScan* scan, AttributeSet** working_attrs) {
    {
        Status s = Verify(scan->scan_, working_attrs);
        if (!s.Ok())
            return s;
    }

    scopes_.push_back(*working_attrs);

    {
        Attribute attr;
        Status s = Verify(scan->expr_, &attr);
        if (!s.Ok())
           return s; 

        if (attr.type != DatumType::Bool) {
            return Status(false, "Analysis Error: where clause expression must evaluate to true or false");
        }
    }

    scan->output_attrs_ = *working_attrs;
    scopes_.pop_back();

    return Status();
}

Status Analyzer::Verify(ProjectScan* scan, AttributeSet** working_attrs) {
    AttributeSet* input_attrs;
    {
        Status s = Verify(scan->input_, &input_attrs);
        if (!s.Ok())
            return s;
    }

    scopes_.push_back(input_attrs);
    scan->input_attrs_ = input_attrs;

    //replace wildcards with actual attribute names
    while (true) { //looping since multiple wildcards may be used, eg 'select *, * from [table];'
        int idx = -1;
        for (size_t i = 0; i < scan->projs_.size(); i++) {
            Expr* e = scan->projs_.at(i);
            if (e->Type() == ExprType::Literal && ((Literal*)e)->t_.type == TokenType::Star) {
                idx = i;
                break; 
            }
        }

        if (idx == -1)
            break;

        scan->projs_.erase(scan->projs_.begin() + idx);
        std::vector<Expr*> attr;
        for (const Attribute& a: input_attrs->GetAttributes()) {
            attr.push_back(new ColRef({ a.rel_ref, a.name }));
        }
        scan->projs_.insert(scan->projs_.begin() + idx, attr.begin(), attr.end());
    }


    //add order columns to projection columns if not already included
    //need this to be able to sort by a column even if that column will not end up in the output
    std::unordered_map<std::string, bool> included;
    for (Expr* e: scan->projs_) {
        included.insert({e->ToString(), true});
    }
    scan->ghost_column_count_ = 0;
    for (OrderCol oc: scan->order_cols_) {
        if (included.find(oc.col->ToString()) == included.end()) {
            included.insert({oc.col->ToString(), true});
            scan->projs_.push_back(oc.col);
            scan->ghost_column_count_++;
        }
    }

    //order cols
    for (OrderCol oc: scan->order_cols_) {
        {
            Attribute attr;
            Status s = Verify(oc.col, &attr);
            if (!s.Ok())
                return s;
        }

        {
            Attribute attr;
            Status s = Verify(oc.asc, &attr);
            if (!s.Ok()) {
                return s;
            }
        }
    }

    //projection
    {
        std::vector<Attribute> attrs;
        std::vector<bool> dummy_not_nulls;
        for (Expr* e: scan->projs_) {
            Attribute attr;
            Status s = Verify(e, &attr);
            if (!s.Ok()) {
                return s;
            }

            attrs.push_back(attr);
            dummy_not_nulls.push_back(false);
        }

        *working_attrs = new AttributeSet(attrs, dummy_not_nulls);
        scan->output_attrs_ = *working_attrs;
    }

    //limit
    {
        Attribute attr;
        Status s = Verify(scan->limit_, &attr);
        if (!s.Ok()) {
            return s;
        }

        if (!Datum::TypeIsInteger(attr.type)) {
            return Status(false, "Error: 'Limit' must be followed by an expression that evaluates to an integer");
        }
    }

    scopes_.pop_back();

    return Status(); 
}

}

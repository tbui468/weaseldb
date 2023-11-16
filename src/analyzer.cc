#include "analyzer.h"
#include "schema.h"

namespace wsldb {

Status Analyzer::Verify(Stmt* stmt, std::vector<DatumType>& types) {
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
            return SelectVerifier((SelectStmt*)stmt, types);
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

Status Analyzer::Verify(Expr* expr, DatumType* type) {
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
        case ExprType::Predict:
            return VerifyPredict((Predict*)expr, type);
        default:
            return Status(false, "Execution Error: Invalid expression type");
    }
}

Status Analyzer::CreateVerifier(CreateStmt* stmt) { 
    Schema* schema;
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
    Status s = GetSchema(stmt->target_.lexeme, &stmt->schema_);
    if (!s.Ok())
        return s;
   
    AttributeSet* working_attrs = stmt->schema_->MakeAttributeSet(stmt->target_.lexeme);

    for (Token t: stmt->attrs_) {
        if (!working_attrs->Contains(stmt->target_.lexeme, t.lexeme)) {
            return Status(false, ("Error: Table does not have a matching column name"));
        }
    }

    size_t attr_count = working_attrs->AttributeCount() - 1; //ignore _rowid since caller doesn't explicitly insert that

    for (const std::vector<Expr*>& tuple: stmt->values_) {
        if (attr_count < tuple.size())
            return Status(false, "Error: Value count must match specified attribute name count");

        std::vector<Expr*> col_assign;
        for (size_t i = 0; i < stmt->attrs_.size(); i++) {
            col_assign.push_back(new ColAssign(stmt->attrs_.at(i), tuple.at(i)));
        }
        stmt->col_assigns_.push_back(col_assign);
    }

    scopes_.push_back(working_attrs);
    for (const std::vector<Expr*>& assigns: stmt->col_assigns_) {
        for (Expr* e: assigns) {
            DatumType type;
            Status s = Verify(e, &type);
            if (!s.Ok())
                return s;
        }
    }
    scopes_.pop_back();

    return Status(); 
}


Status Analyzer::UpdateVerifier(UpdateStmt* stmt) { 
    Status s = GetSchema(stmt->target_.lexeme, &stmt->schema_);
    if (!s.Ok())
        return s;
   
    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->scan_, &working_attrs);
        if (!s.Ok())
            return s;
    }

    scopes_.push_back(working_attrs);

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

    scopes_.pop_back();
    return Status(); 
}


Status Analyzer::DeleteVerifier(DeleteStmt* stmt) { 
    Status s = GetSchema(stmt->target_.lexeme, &stmt->schema_);
    if (!s.Ok())
        return s;

    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->scan_, &working_attrs);
        if (!s.Ok())
            return s;
    }

    scopes_.push_back(working_attrs);

    {
        DatumType type;
        Status s = Verify(stmt->where_clause_, &type);
        if (!s.Ok())
            return s;
        if (type != DatumType::Bool)
            return Status(false, "Error: 'where' clause must evaluated to a boolean value");
    }

    scopes_.pop_back();

    return Status(); 
}

Status Analyzer::SelectVerifier(SelectStmt* stmt, std::vector<DatumType>& types) { 
    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->target_, &working_attrs);
        if (!s.Ok())
            return s;
    }

    scopes_.push_back(working_attrs);

    //projection
    for (Expr* e: stmt->projs_) {
        DatumType type;
        Status s = Verify(e, &type);
        if (!s.Ok()) {
            return s;
        }
        types.push_back(type);

        //fill in row description for usage during execution stage
        stmt->row_description_.emplace_back("?rel_ref?", e->ToString(), type, false);
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

    scopes_.pop_back();

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

Status Analyzer::VerifyLiteral(Literal* expr, DatumType* type) { 
    *type = LiteralTokenToDatumType(expr->t_.type);
    return Status(); 
}

Status Analyzer::VerifyBinary(Binary* expr, DatumType* type) { 
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

Status Analyzer::VerifyUnary(Unary* expr, DatumType* type) { 
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

Status Analyzer::VerifyColRef(ColRef* expr, DatumType* type) { 
    Attribute a;
    Status s = GetAttribute(&a, &expr->idx_, &expr->scope_, expr->table_ref_, expr->t_.lexeme);
    if (!s.Ok())
        return s;

    *type = a.type;

    return Status(); 
}

Status Analyzer::VerifyColAssign(ColAssign* expr, DatumType* type) { 
    DatumType right_type;
    {
        Status s = Verify(expr->right_, &right_type);
        if (!s.Ok())
            return s;
    }

    Attribute a;
    {
        std::string table_ref = "";
        Status s = GetAttribute(&a, &expr->idx_, &expr->scope_, table_ref, expr->col_.lexeme);
        if (!s.Ok())
            return s;
    }

    {
        Status s = a.CheckConstraints(right_type);
        if (!s.Ok())
            return s;
    }

    *type = a.type;

    return Status(); 
}

Status Analyzer::VerifyCall(Call* expr, DatumType* type) { 
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

Status Analyzer::VerifyIsNull(IsNull* expr, DatumType* type) { 
    *type = DatumType::Bool;

    DatumType left_type;
    Status s = Verify(expr->left_, &left_type);
    if (!s.Ok())
        return s;

    return Status(); 
}

Status Analyzer::VerifyScalarSubquery(ScalarSubquery* expr, DatumType* type) { 
    std::vector<DatumType> types;
    {
        Status s = Verify(expr->stmt_, types);
        if (!s.Ok())
            return s;
    }

    //TODO: this should be taken from GetQueryState()->attrs
    //at this point we don't know if the working table is a constant or physical or other table type
    //so how can we set *evaluated_type for type checking?
    if (types.size() != 1)
        return Status(false, "Error: Scalar subquery must return a single value");

    *type = types.at(0);

    return Status();
}

Status Analyzer::VerifyPredict(Predict* expr, DatumType* type) {
    {
        DatumType arg_type;
        Status s = Verify(expr->arg_, &arg_type);
        if (!s.Ok())
            return s;

        *type = DatumType::Int8; //TODO: temp to test mnist
    }

    {
        std::string serialized_model;
        Status s = (*txn_)->Get(Storage::Models(), expr->model_name_.lexeme, &serialized_model);
        if (!s.Ok())
            return Status(false, "Analysis Error: Model with the name '" + expr->model_name_.lexeme + "' does not exist");
    }

    return Status();
}

Status Analyzer::Verify(WorkTable* scan, AttributeSet** working_attrs) {
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

Status Analyzer::VerifyLeft(LeftJoin* scan, AttributeSet** working_attrs) {
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

    scan->right_attr_count_ = right_attrs->AttributeCount();

    {
        bool has_duplicate_tables;
        *working_attrs = new AttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
        scan->attrs_ = *working_attrs;
        if (has_duplicate_tables)
            return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
    }

    scopes_.push_back(*working_attrs);
    {
        DatumType type;
        Status s = Verify(scan->condition_, &type);
        if (!s.Ok())
            return s;

        if (type != DatumType::Bool) {
            return Status(false, "Error: Inner join condition must evaluate to a boolean type");
        }
    }
    scopes_.pop_back();

    return Status();
}

Status Analyzer::VerifyFull(FullJoin* scan, AttributeSet** working_attrs) {
    Status s = Verify(scan->right_join_, working_attrs);
    scan->attrs_ = *working_attrs;

    if (!s.Ok())
        return s;

    scan->attr_count_ = (*working_attrs)->AttributeCount();

    return Status();
}

Status Analyzer::VerifyInner(InnerJoin* scan, AttributeSet** working_attrs) {
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
        scan->attrs_ = *working_attrs;
        if (has_duplicate_tables)
            return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
    }

    //Need to put current attributeset into QueryState temporarily so that Expr::Analyze
    //can use that data to perform semantic analysis for 'on' clause.  Normally Expr::Analyze is only
    //called once entire WorkTable is Analyzed an at least a single AttributeSet is in QueryState,
    //but InnerJoins have an Expr embedded as part of the WorkTable (the 'on' clause), so this is needed
    //Something similar is done in InnerJoin::NextRow
    scopes_.push_back(*working_attrs);
    {
        DatumType type;
        Status s = Verify(scan->condition_, &type);
        if (!s.Ok())
            return s;

        if (type != DatumType::Bool) {
            return Status(false, "Error: Inner join condition must evaluate to a boolean type");
        }
    }
    scopes_.pop_back();

    return Status(true, "ok");
}

Status Analyzer::VerifyCross(CrossJoin* scan, AttributeSet** working_attrs) {
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
        scan->attrs_ = *working_attrs;
        if (has_duplicate_tables)
            return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
    }

    return Status();
}

Status Analyzer::VerifyConstant(ConstantTable* scan, AttributeSet** working_attrs) {
    std::vector<std::string> names;
    std::vector<DatumType> types;
    std::vector<bool> not_nulls;
    for (Expr* e: scan->target_cols_) {
        DatumType type;
        Status s = Verify(e, &type);
        if (!s.Ok())
            return s;
        names.push_back("?col?");
        types.push_back(type);
        not_nulls.push_back(true);
    }

    *working_attrs = new AttributeSet("?table?", names, types, not_nulls);
    scan->attrs_ = *working_attrs;

    return Status();
}

Status Analyzer::VerifyTable(PrimaryTable* scan, AttributeSet** working_attrs) {
    bool ok = GetSchema(scan->tab_name_, &scan->schema_).Ok();

    if (!ok) {
        return Status(false, "Error: Table '" + scan->tab_name_ + "' does not exist");
    }

    *working_attrs = scan->schema_->MakeAttributeSet(scan->ref_name_);
    scan->attrs_ = *working_attrs;

    return Status();
}

}

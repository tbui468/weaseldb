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
        case ExprType::Cast:
            return VerifyCast((Cast*)expr, type);
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
    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->scan_, &working_attrs);
        if (!s.Ok()) return s;
    }

    std::vector<std::string> tables = working_attrs->TableNames();
    if (tables.size() != 1) {
        return Status(false, "Analysis Error: Cannot insert into more than a single table");
    }

    for (Token t: stmt->attrs_) {
        if (!working_attrs->Contains(tables.at(0), t.lexeme)) {
            return Status(false, "Error: Table does not have a matching column '" + t.lexeme + "'");
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
    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->scan_, &working_attrs);
        if (!s.Ok())
            return s;
    }

    scopes_.push_back(working_attrs);
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
    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->scan_, &working_attrs);
        if (!s.Ok())
            return s;
    }

    return Status(); 
}

Status Analyzer::SelectVerifier(SelectStmt* stmt, std::vector<DatumType>& types) {
    AttributeSet* working_attrs;
    {
        Status s = Verify(stmt->scan_, &working_attrs);
        if (!s.Ok()) return s;
    }

    for (const Attribute& a: working_attrs->GetAttributes()) {
        types.push_back(a.type);
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
        Status s;
        for (AttributeSet* as: scopes_) {
            int dummy_idx;
            s = as->GetAttribute(table_ref, expr->col_.lexeme, &a, &dummy_idx);
            if (s.Ok())
                break;
        }
        if (!s.Ok())
            return s;
    }

    {
        Status s = a.CheckConstraints(right_type);
        if (!s.Ok())
            return s;
    }

    *type = a.type;
    expr->field_type_ = a.type;

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

Status Analyzer::VerifyCast(Cast* expr, DatumType* type) {
    DatumType value_type;
    {
        Status s = Verify(expr->value_, &value_type);
        if (!s.Ok())
            return s;
    }

    DatumType target_type = TypeTokenToDatumType(expr->type_.type);

    if (Datum::CanCast(value_type, target_type)) {
        *type = target_type;
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

Status Analyzer::VerifyTable(TableScan* scan, AttributeSet** working_attrs) {
    bool ok = GetSchema(scan->tab_name_, &scan->schema_).Ok();

    if (!ok) {
        return Status(false, "Error: Table '" + scan->tab_name_ + "' does not exist");
    }

    *working_attrs = scan->schema_->MakeAttributeSet(scan->ref_name_);
    scan->attrs_ = *working_attrs;

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
        DatumType type;
        Status s = Verify(scan->expr_, &type);
        if (!s.Ok())
           return s; 

        if (type != DatumType::Bool) {
            return Status(false, "Analysis Error: where clause expression must evaluate to true or false");
        }
    }
    scan->attrs_ = *working_attrs;

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
        scan->attrs_ = *working_attrs;
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
        DatumType type;
        Status s = Verify(scan->expr_, &type);
        if (!s.Ok())
           return s; 

        if (type != DatumType::Bool) {
            return Status(false, "Analysis Error: where clause expression must evaluate to true or false");
        }
    }

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
            attr.push_back(new ColRef(Token(a.name, TokenType::Identifier), Token(a.rel_ref, TokenType::Identifier)));
        }
        scan->projs_.insert(scan->projs_.begin() + idx, attr.begin(), attr.end());
    }

    //projection
    {
        std::vector<std::string> names;
        std::vector<DatumType> types;
        std::vector<bool> not_nulls;
        for (Expr* e: scan->projs_) {
            DatumType type;
            Status s = Verify(e, &type);
            if (!s.Ok()) {
                return s;
            }

            names.push_back(e->ToString());
            types.push_back(type);
            not_nulls.push_back(false); //dummy value that is not used
        }

        *working_attrs = new AttributeSet("?rel_ref?", names, types, not_nulls);
        scan->attrs_ = (*working_attrs)->GetAttributes();
    }

    //order cols
    for (OrderCol oc: scan->order_cols_) {
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
        Status s = Verify(scan->limit_, &type);
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

}

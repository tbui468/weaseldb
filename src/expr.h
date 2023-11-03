#pragma once

#include <string>
#include <iostream>
#include <algorithm>
#include "token.h"
#include "datum.h"
#include "row.h"
#include "table.h"
#include "status.h"
#include "storage.h"

namespace wsldb {

//each query (and any subqueries nested inside) is associated with a single QueryState object
//this state holds the database handle, and also a stack of attributes for use during semantic analysis
//in case an inner subquery accesses a column in the outer query.  Similarly, a stack of the outer row(s)
//is stored during execution in case a subquery needs access to a column in the outer scope.
struct QueryState {
public:
    QueryState(Storage* storage, Batch* batch): storage(storage), batch(batch), scan_idx(0) {
        ResetAggState();
    }

    inline WorkingAttributeSet* AnalysisScopesTop() const {
        return analysis_scopes_.back();
    }
    inline void PushAnalysisScope(WorkingAttributeSet* as) {
        analysis_scopes_.push_back(as);
    }
    inline void PopAnalysisScope() {
        analysis_scopes_.pop_back();
    }
    
    inline Row* ScopeRowAt(int scope) {
        return scope_rows_.at(scope_rows_.size() - 1 - scope);
    }
    inline void PushScopeRow(Row* r) {
        scope_rows_.push_back(r);
    }
    inline void PopScopeRow() {
        scope_rows_.pop_back();
    }

    inline void ResetAggState() {
        is_agg = false;
        first_ = true;
        sum_ = Datum(0);
        count_ = Datum(0);
    }

    Status GetWorkingAttribute(WorkingAttribute* attr, const std::string& table_ref_param, const std::string& col) const {
        for (int i = analysis_scopes_.size() - 1; i >= 0; i--) {
            Status s = GetWorkingAttributeAtScopeOffset(attr, table_ref_param, col, i);
            if (s.Ok() || i == 0)
                return s;
        }

        return Status(false, "should never reach this"); //keeping compiler quiet
    }
private:
    Status GetWorkingAttributeAtScopeOffset(WorkingAttribute* attr, const std::string& table_ref_param, const std::string& col, int i) const {
        std::string table_ref = table_ref_param;
        WorkingAttributeSet* as = analysis_scopes_.at(i);
        if (table_ref == "") {
            std::vector<std::string> tables;
            for (const std::string& name: as->TableNames()) {
                if (as->Contains(name, col))
                    tables.push_back(name);
            }

            if (tables.size() > 1) {
                return Status(false, "Error: Column '" + col + "' can refer to columns in muliple tables.");
            } else if (tables.empty()) {
                return Status(false, "Error: Column '" + col + "' does not exist");
            } else {
                table_ref = tables.at(0);
            }
        }

        if (!as->Contains(table_ref, col)) {
            return Status(false, "Error: Column '" + table_ref + "." + col + "' does not exist");
        }

        *attr = as->GetWorkingAttribute(table_ref, col);
        //0 is current scope, 1 is the immediate outer scope, 2 is two outer scopes out, etc
        attr->scope = analysis_scopes_.size() - 1 - i;
        
        return Status(true, "ok");
    }
public:
    Storage* storage;
    Batch* batch;
    bool is_agg;
    //state for aggregate functions
    Datum min_;
    Datum max_;
    Datum sum_;
    Datum count_;
    bool first_;
    int scan_idx;
private:
    std::vector<WorkingAttributeSet*> analysis_scopes_;
    std::vector<Row*> scope_rows_;
};

enum class StmtType {
    Create,
    Insert,
    Update,
    Delete,
    Select,
    DescribeTable,
    DropTable
};

//Putting class Stmt here since we need it in Expr,
//but putting it in stmt.h would cause a circular dependency
class Stmt {
public:
    virtual Status Analyze(QueryState& qs, std::vector<DatumType>& types) = 0;
    virtual Status Execute(QueryState& qs) = 0;
    Status OpenTable(QueryState& qs, const std::string& table_name, Table** table) {
        std::string serialized_table;
        TableHandle catalogue = qs.storage->GetTable(qs.storage->CatalogueTableName());
        bool ok = catalogue.db->Get(rocksdb::ReadOptions(), table_name, &serialized_table).ok();

        if (!ok)
            return Status(false, "Error: Table doesn't exist");

        *table = new Table(table_name, serialized_table);

        return Status(true, "ok");
    }
    virtual StmtType Type() const;
};

//QueryState is saved in both Expr and Stmt, could probably
//make a single class called QueryNode to store this, and have both Expr and Stmt inherit from that
class Expr {
public:
    //TODO: reuslt and is_agg can be put inside of query state
    virtual inline Status Eval(QueryState& qs, Row* r, Datum* result) = 0;
    virtual std::string ToString() = 0;
    virtual Status Analyze(QueryState& qs, DatumType* evaluated_type) = 0;
};

class Literal: public Expr {
public:
    Literal(Token t): t_(t) {}
    Literal(bool b): t_(b ? Token("true", TokenType::TrueLiteral) : Token("false", TokenType::FalseLiteral)) {}
    Literal(int i): t_(Token(std::to_string(i), TokenType::IntLiteral)) {}
    inline Status Eval(QueryState& qs, Row* r, Datum* result) override {
        qs.is_agg = false;
        *result = Datum(LiteralTokenToDatumType(t_.type), t_.lexeme);
        return Status(true, "ok");
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    Status Analyze(QueryState& qs, DatumType* evaluated_type) override {
        *evaluated_type = Datum(LiteralTokenToDatumType(t_.type), t_.lexeme).Type();
        return Status(true, "ok");
    }
private:
    Token t_;
};

class Binary: public Expr {
public:
    Binary(Token op, Expr* left, Expr* right):
        op_(op), left_(left), right_(right) {}
    inline Status Eval(QueryState& qs, Row* row, Datum* result) override {
        qs.is_agg = false;

        Datum l;
        Status s1 = left_->Eval(qs, row, &l);
        if (!s1.Ok()) return s1;

        Datum r;
        Status s2 = right_->Eval(qs, row, &r);
        if (!s2.Ok()) return s2;

        if (l.IsType(DatumType::Null) || r.IsType(DatumType::Null)) {
            *result = Datum();
            return Status();
        }

        switch (op_.type) {
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
    std::string ToString() override {
        return "(" + op_.lexeme + " " + left_->ToString() + " " + right_->ToString() + ")";
    }
    Status Analyze(QueryState& qs, DatumType* evaluated_type) override {
        DatumType left_type;
        {
            Status s = left_->Analyze(qs, &left_type);
            if (!s.Ok())
                return s;
        }
        DatumType right_type;
        {
            Status s = right_->Analyze(qs, &right_type);
            if (!s.Ok())
                return s;
        }

        if (left_type == DatumType::Null || right_type == DatumType::Null) {
            *evaluated_type = DatumType::Null;
            return Status();
        }

        switch (op_.type) {
            case TokenType::Equal:
            case TokenType::NotEqual:
            case TokenType::Less:
            case TokenType::LessEqual:
            case TokenType::Greater:
            case TokenType::GreaterEqual:
                if (!(Datum::TypeIsNumeric(left_type) && Datum::TypeIsNumeric(right_type)) && left_type != right_type) {
                        return Status(false, "Error: Equality and relational operands must be same data types");
                }
                *evaluated_type = DatumType::Bool;
                break;
            case TokenType::Or:
            case TokenType::And:
                if (!(left_type == DatumType::Bool && right_type == DatumType::Bool)) {
                    return Status(false, "Error: Logical operator operands must be boolean types");
                }
                *evaluated_type = DatumType::Bool;
                break;
            case TokenType::Plus:
            case TokenType::Minus:
            case TokenType::Star:
            case TokenType::Slash:
                if (!(Datum::TypeIsNumeric(left_type) && Datum::TypeIsNumeric(right_type))) {
                    return Status(false, "Error: The '" + op_.lexeme + "' operator operands must both be a numeric type");
                } 
                *evaluated_type = left_type;
                break;
            default:
                return Status(false, "Implementation Error: op type not implemented in Binary expr!");
                break;
        }

        return Status();
    }
private:
    Token op_;
    Expr* left_;
    Expr* right_;
};

class Unary: public Expr {
public:
    Unary(Token op, Expr* right): op_(op), right_(right) {}
    inline Status Eval(QueryState& qs, Row* r, Datum* result) override {
        qs.is_agg = false;

        Datum right;
        Status s = right_->Eval(qs, r, &right);
        if (!s.Ok()) return s;

        switch (op_.type) {
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
        return Status(true, "ok");
    }
    std::string ToString() override {
        return "(" + op_.lexeme + " " + right_->ToString() + ")";
    }
    Status Analyze(QueryState& qs, DatumType* evaluated_type) override {
        DatumType type;
        {
            Status s = right_->Analyze(qs, &type);
            if (!s.Ok())
                return s;
        }
        switch (op_.type) {
            case TokenType::Not:
                if (type != DatumType::Bool) {
                    return Status(false, "Error: 'not' operand must be a boolean type.");
                }
                *evaluated_type = DatumType::Bool;
                break;
            case TokenType::Minus:
                if (!Datum::TypeIsNumeric(type)) {
                    return Status(false, "Error: '-' operator operand must be numeric type");
                }
                *evaluated_type = type;
                break;
            default:
                return Status(false, "Implementation Error: op type not implemented in Binary expr!");
                break;
        }

        return Status(true, "ok");
    }
private:
    Token op_;
    Expr* right_;
};


class ColRef: public Expr {
public:
    ColRef(Token t): t_(t), table_ref_(""), idx_(-1), scope_(-1) {}
    ColRef(Token t, Token table_ref): t_(t), table_ref_(table_ref.lexeme), idx_(-1), scope_(-1) {}
    inline Status Eval(QueryState& qs, Row* r, Datum* result) override {
        qs.is_agg = false;
        *result = qs.ScopeRowAt(scope_)->data_.at(idx_);
        return Status(true, "ok");
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    Status Analyze(QueryState& qs, DatumType* evaluated_type) override {
        WorkingAttribute a;
        Status s = qs.GetWorkingAttribute(&a, table_ref_, t_.lexeme);
        if (!s.Ok())
            return s;

        idx_ = a.idx;
        *evaluated_type = a.type;
        scope_ = a.scope;

        return Status(true, "ok");
    }
private:
    Token t_;
    std::string table_ref_;
    int idx_;
    int scope_;
};


//Used for both InsertStmt and UpdateStmt
class ColAssign: public Expr {
public:
    ColAssign(Token col, Expr* right): 
        col_(col), right_(right), scope_(-1), idx_(-1) {}
    inline Status Eval(QueryState& qs, Row* r, Datum* result) override {
        qs.is_agg = false;

        Datum right;
        Status s = right_->Eval(qs, r, &right);
        if (!s.Ok()) return s;

        r->data_.at(idx_) = right;

        *result = right; //result not used
        return Status(true, "ok");
    }
    std::string ToString() override {
        return "(:= " + col_.lexeme + " " + right_->ToString() + ")";
    }
    Status Analyze(QueryState& qs, DatumType* evaluated_type) override {
        DatumType type;
        {
            Status s = right_->Analyze(qs, &type);
            if (!s.Ok())
                return s;
        }

        WorkingAttribute a;
        {
            std::string table_ref = "";
            Status s = qs.GetWorkingAttribute(&a, table_ref, col_.lexeme);
            if (!s.Ok())
                return s;
        }

        {
            Status s = a.CheckConstraints(type);
            if (!s.Ok())
                return s;
        }

        *evaluated_type = a.type;
        idx_ = a.idx;
        scope_ = a.scope;

        return Status(true, "ok");
    }
private:
    Token col_;
    Expr* right_;
    int scope_;
    int idx_;
};

struct OrderCol {
    Expr* col;
    Expr* asc;
};

struct Call: public Expr {
public:
    Call(Token fcn, Expr* arg): fcn_(fcn), arg_(arg) {}
    inline Status Eval(QueryState& qs, Row* r, Datum* result) override {

        Datum arg;
        Status s = arg_->Eval(qs, r, &arg);
        if (!s.Ok())
            return s;

        switch (fcn_.type) {
            case TokenType::Avg:
                qs.sum_ += arg;
                qs.count_ += Datum(static_cast<int64_t>(1));
                *result = qs.sum_ / qs.count_;
                break;
            case TokenType::Count: {
                qs.count_ += Datum(static_cast<int64_t>(1));
                *result = qs.count_;
                break;
            }
            case TokenType::Max:
                if (qs.first_ || arg > qs.max_) {
                    qs.max_ = arg;
                    qs.first_ = false;
                }
                *result = qs.max_;
                break;
            case TokenType::Min:
                if (qs.first_ || arg < qs.min_) {
                    qs.min_ = arg;
                    qs.first_ = false;
                }
                *result = qs.min_;
                break;
            case TokenType::Sum:
                qs.sum_ += arg;
                *result = qs.sum_;
                break;
            default:
                return Status(false, "Error: Invalid function name");
                break;
        }

        //Calling Eval on argument may set qs.is_agg to false, so need to set it after evaluating argument
        qs.is_agg = true;
        return Status(true, "ok");
    }
    std::string ToString() override {
        return fcn_.lexeme + arg_->ToString();
    }
    Status Analyze(QueryState& qs, DatumType* evaluated_type) override {
        DatumType type;
        {
            Status s = arg_->Analyze(qs, &type);
            if (!s.Ok()) {
                return s;
            }
        }

        switch (fcn_.type) {
            case TokenType::Avg:
                //TODO: need to think about how we want to deal with integer division
                //using floor division now if argument is integer type - what does postgres do?
                *evaluated_type = type;
                break;
            case TokenType::Count:
                *evaluated_type = DatumType::Int8;
                break;
            case TokenType::Max:
            case TokenType::Min:
            case TokenType::Sum:
                *evaluated_type = type;
                break;
            default:
                return Status(false, "Error: Invalid function name");
                break;
        }

        if (!TokenTypeIsAggregateFunction(fcn_.type)) {
            return Status(false, "Error: Function '" + fcn_.lexeme + "' does not exist");
        }

        return Status();
    }
private:
    Token fcn_;
    Expr* arg_;
};

class IsNull: public Expr {
public:
    IsNull(Expr* left): left_(left) {}
    inline Status Eval(QueryState& qs, Row* r, Datum* result) override {
        qs.is_agg = false;

        Datum d;
        Status s = left_->Eval(qs, r, &d);
        if (!s.Ok())
            return s;

        if (d.IsType(DatumType::Null)) {
            *result = Datum(true);
        } else {
            *result = Datum(false);
        }

        return Status(true, "ok");
    }
    std::string ToString() override {
        return "IsNull";
    }
    Status Analyze(QueryState& qs, DatumType* evaluated_type) override {
        *evaluated_type = DatumType::Bool;

        DatumType left_type;
        Status s = left_->Analyze(qs, &left_type);
        if (!s.Ok())
            return s;

        return Status(true, "ok");
    }
private:
    Expr* left_;
};

class IsNotNull: public Expr {
public:
    IsNotNull(Expr* left): left_(left) {}
    inline Status Eval(QueryState& qs, Row* r, Datum* result) override {
        qs.is_agg = false;

        Datum d;
        Status s = left_->Eval(qs, r, &d);
        if (!s.Ok())
            return s;

        if (!d.IsType(DatumType::Null)) {
            *result = Datum(true);
        } else {
            *result = Datum(false);
        }

        return Status(true, "ok");
    }
    std::string ToString() override {
        return "IsNotNull";
    }
    Status Analyze(QueryState& qs, DatumType* evaluated_type) override {
        *evaluated_type = DatumType::Bool;

        DatumType left_type;
        Status s = left_->Analyze(qs, &left_type);
        if (!s.Ok())
            return s;

        return Status(true, "ok");
    }
private:
    Expr* left_;
};

class ScalarSubquery: public Expr {
public:
    ScalarSubquery(Stmt* stmt): stmt_(stmt) {}
    inline Status Eval(QueryState& qs, Row* r, Datum* result) override {
        qs.is_agg = false;

        Status s = stmt_->Execute(qs);
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
    std::string ToString() override {
        return "scalar subquery";
    }
    Status Analyze(QueryState& qs, DatumType* evaluated_type) override {
        std::vector<DatumType> types;
        {
            Status s = stmt_->Analyze(qs, types);
            if (!s.Ok())
                return s;
        }

        //TODO: this should be taken from GetQueryState()->attrs
        //at this point we don't know if the working table is a constant or physical or other table type
        //so how can we set *evaluated_type for type checking?
        if (types.size() != 1)
            return Status(false, "Error: Scalar subquery must return a single value");

        *evaluated_type = types.at(0);

        return Status();
    }
private:
    Stmt* stmt_;
};

class WorkTable {
public:
    virtual Status Analyze(QueryState& qs, WorkingAttributeSet** working_attrs) = 0;
    virtual Status BeginScan(QueryState& qs) = 0;
    virtual Status NextRow(QueryState& qs, Row** r) = 0;
    std::vector<Attribute> GetAttributes() const {
        return attrs_->GetAttributes();
    }
protected:
    WorkingAttributeSet* attrs_ { nullptr };
};

class LeftJoin: public WorkTable {
public:
    LeftJoin(WorkTable* left, WorkTable* right, Expr* condition): left_(left), right_(right), condition_(condition) {}
    Status Analyze(QueryState& qs, WorkingAttributeSet** working_attrs) override {
        WorkingAttributeSet* left_attrs;
        {
            Status s = left_->Analyze(qs, &left_attrs);
            if (!s.Ok())
                return s;
        }

        WorkingAttributeSet* right_attrs;
        {
            Status s = right_->Analyze(qs, &right_attrs);
            if (!s.Ok())
                return s;
        }

        right_attr_count_ = right_attrs->WorkingAttributeCount();

        {
            bool has_duplicate_tables;
            *working_attrs = new WorkingAttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
            attrs_ = *working_attrs;
            if (has_duplicate_tables)
                return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
        }

        qs.PushAnalysisScope(*working_attrs);
        {
            DatumType type;
            Status s = condition_->Analyze(qs, &type);
            if (!s.Ok())
                return s;

            if (type != DatumType::Bool) {
                return Status(false, "Error: Inner join condition must evaluate to a boolean type");
            }
        }
        qs.PopAnalysisScope();

        return Status(true, "ok");
    }
    Status BeginScan(QueryState& qs) override {
        left_->BeginScan(qs);
        right_->BeginScan(qs);

        //initialize left row
        Status s = left_->NextRow(qs, &left_row_);
        lefts_inserted_ = 0;
        if (!s.Ok())
            return Status(false, "No more rows");

        return Status(true, "ok");
    }
    Status NextRow(QueryState& qs, Row** r) override {
        Row* right_row;

        while (true) {
            Status s = right_->NextRow(qs, &right_row);
            if (!s.Ok()) {
                //get new left
                {
                    if (lefts_inserted_ == 0) {
                        std::vector<Datum> result = left_row_->data_;
                        for (int i = 0; i < right_attr_count_; i++) {
                            result.push_back(Datum());
                        }

                        *r = new Row(result);
                        lefts_inserted_++;

                        return Status(true, "ok");
                    }

                    Status s = left_->NextRow(qs, &left_row_);
                    lefts_inserted_ = 0;
                    if (!s.Ok())
                        return Status(false, "No more rows");
                }

                {
                    right_->BeginScan(qs);
                    Status s = right_->NextRow(qs, &right_row);
                    if (!s.Ok())
                        return Status(false, "No more rows");
                } 
            }

            {
                std::vector<Datum> result = left_row_->data_;
                result.insert(result.end(), right_row->data_.begin(), right_row->data_.end());

                *r = new Row(result);
                Datum d;

                qs.PushScopeRow(*r);
                Status s = condition_->Eval(qs, *r, &d);
                qs.PopScopeRow();

                if (d.AsBool()) {
                    lefts_inserted_++;
                    return Status(true, "ok");
                }

            }
        }

        return Status(false, "Should never see this message");
    }
private:
    //used to track number of times a given left row is inserted into the final working table
    //if a row is inserted 0 times, then insert the left row and fill in the right row with nulls
    int lefts_inserted_;
    int right_attr_count_;
    Row* left_row_;
    WorkTable* left_;
    WorkTable* right_;
    Expr* condition_;
};

//Idea: make a left join, run that to get left rows + rows in common
//Then run the right join, but only include rights with left sides that are nulled out
class FullJoin: public WorkTable {
public:
    FullJoin(WorkTable* left, WorkTable* right, Expr* condition): 
        left_(left), right_(right), condition_(condition), right_join_(new LeftJoin(right, left, condition)) {}

    Status Analyze(QueryState& qs, WorkingAttributeSet** working_attrs) override {
        Status s = right_join_->Analyze(qs, working_attrs);
        attrs_ = *working_attrs;

        if (!s.Ok())
            return s;

        attr_count_ = (*working_attrs)->WorkingAttributeCount();

        return s;
    }
    Status BeginScan(QueryState& qs) override {
        right_join_->BeginScan(qs);
        do_right_ = true;

        return Status(true, "ok");
    }
    Status NextRow(QueryState& qs, Row** r) override {
        //attempt grabbing row from right_join_
        //if none left, initialize left_ and right_, and start processing left join but only use values with nulled right sides
        if (do_right_) {
            Status s = right_join_->NextRow(qs, r);
            if (s.Ok()) {
                return s;
            } else {
                //reset to begin augmented left outer join scan
                do_right_ = false;
                left_->BeginScan(qs);
                right_->BeginScan(qs);
                Status s = left_->NextRow(qs, &left_row_);
                lefts_inserted_ = 0;
                if (!s.Ok())
                    return Status(false, "No more rows");
            }
        }
        

        //right join is done, so grabbing left join rows where the right side has no match, and will be nulled
        Row* right_row;

        while (true) {
            Status s = right_->NextRow(qs, &right_row);
            if (!s.Ok()) {
                //get new left
                if (lefts_inserted_ == 0) {
                    std::vector<Datum> result;
                    int right_attr_count = attr_count_ - left_row_->data_.size();
                    for (int i = 0; i < right_attr_count; i++) {
                        result.push_back(Datum());
                    }

                    result.insert(result.end(), left_row_->data_.begin(), left_row_->data_.end());

                    *r = new Row(result);
                    lefts_inserted_++;

                    return Status(true, "ok");
                }

                {
                    Status s = left_->NextRow(qs, &left_row_);
                    lefts_inserted_ = 0;
                    if (!s.Ok())
                        return Status(false, "No more rows");
                }

                {
                    right_->BeginScan(qs);
                    Status s = right_->NextRow(qs, &right_row);
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
                result.insert(result.end(), left_row_->data_.begin(), left_row_->data_.end());

                Row temp(result);
                Datum d;

                qs.PushScopeRow(&temp);
                Status s = condition_->Eval(qs, &temp, &d);
                qs.PopScopeRow();

                //TODO: optimization opportunity here
                //if a left join row has a matching right side here, no need to check the
                //rest of the right rows to determine if a left + nulled-right is necessary
                //can go straight to the next left row immediately
                if (d.AsBool()) {
                    lefts_inserted_++;
                }

            }
        }

        return Status(false, "Should never see this message");
    }
private:
    //used to track number of times a given left row is inserted into the final working table
    //if a row is inserted 0 times, then insert the left row and fill in the right row with nulls
    int lefts_inserted_;
    int attr_count_;
    bool do_right_;
    Row* left_row_;
    WorkTable* left_;
    WorkTable* right_;
    Expr* condition_;
    LeftJoin* right_join_;
};



class InnerJoin: public WorkTable {
public:
    InnerJoin(WorkTable* left, WorkTable* right, Expr* condition): left_(left), right_(right), condition_(condition) {}
    Status Analyze(QueryState& qs, WorkingAttributeSet** working_attrs) override {
        WorkingAttributeSet* left_attrs;
        {
            Status s = left_->Analyze(qs, &left_attrs);
            if (!s.Ok())
                return s;
        }

        WorkingAttributeSet* right_attrs;
        {
            Status s = right_->Analyze(qs, &right_attrs);
            if (!s.Ok())
                return s;
        }

        {
            bool has_duplicate_tables;
            *working_attrs = new WorkingAttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
            attrs_ = *working_attrs;
            if (has_duplicate_tables)
                return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
        }

        //Need to put current attributeset into QueryState temporarily so that Expr::Analyze
        //can use that data to perform semantic analysis for 'on' clause.  Normally Expr::Analyze is only
        //called once entire WorkTable is Analyzed an at least a single WorkingAttributeSet is in QueryState,
        //but InnerJoins have an Expr embedded as part of the WorkTable (the 'on' clause), so this is needed
        //Something similar is done in InnerJoin::NextRow
        qs.PushAnalysisScope(*working_attrs);
        {
            DatumType type;
            Status s = condition_->Analyze(qs, &type);
            if (!s.Ok())
                return s;

            if (type != DatumType::Bool) {
                return Status(false, "Error: Inner join condition must evaluate to a boolean type");
            }
        }
        qs.PopAnalysisScope();

        return Status(true, "ok");
    }
    Status BeginScan(QueryState& qs) override {
        left_->BeginScan(qs);
        right_->BeginScan(qs);

        //initialize left row
        Status s = left_->NextRow(qs, &left_row_);
        if (!s.Ok())
            return Status(false, "No more rows");

        return Status(true, "ok");
    }
    Status NextRow(QueryState& qs, Row** r) override {
        Row* right_row;

        while (true) {
            Status s = right_->NextRow(qs, &right_row);
            if (!s.Ok()) {
                //get new left
                {
                    Status s = left_->NextRow(qs, &left_row_);
                    if (!s.Ok())
                        return Status(false, "No more rows");
                }

                {
                    right_->BeginScan(qs);
                    Status s = right_->NextRow(qs, &right_row);
                    if (!s.Ok())
                        return Status(false, "No more rows");
                } 
            }

            //return concatenated row if condition is met - otherwise continue loop
            {
                std::vector<Datum> result = left_row_->data_;
                result.insert(result.end(), right_row->data_.begin(), right_row->data_.end());

                *r = new Row(result);
                Datum d;

                //this is ugly, we need to do this since Expr::Eval is needed to generate a working table
                //something similar is done in InnerJoin::Analyze
                qs.PushScopeRow(*r);
                Status s = condition_->Eval(qs, *r, &d);
                qs.PopScopeRow();

                if (d.AsBool())
                    return Status(true, "ok");

            }
        }

        return Status(false, "Should never see this message");
    }
private:
    Row* left_row_;
    WorkTable* left_;
    WorkTable* right_;
    Expr* condition_;
};

class CrossJoin: public WorkTable {
public:
    CrossJoin(WorkTable* left, WorkTable* right): left_(left), right_(right), left_row_(nullptr) {}
    Status Analyze(QueryState& qs, WorkingAttributeSet** working_attrs) override {
        WorkingAttributeSet* left_attrs;
        {
            Status s = left_->Analyze(qs, &left_attrs);
            if (!s.Ok())
                return s;
        }

        WorkingAttributeSet* right_attrs;
        {
            Status s = right_->Analyze(qs, &right_attrs);
            if (!s.Ok())
                return s;
        }

        {
            bool has_duplicate_tables;
            *working_attrs = new WorkingAttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
            attrs_ = *working_attrs;
            if (has_duplicate_tables)
                return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
        }

        return Status(true, "ok");
    }
    Status BeginScan(QueryState& qs) override {
        left_->BeginScan(qs);
        right_->BeginScan(qs);

        //initialize left row
        Status s = left_->NextRow(qs, &left_row_);
        if (!s.Ok())
            return Status(false, "No more rows");

        return Status(true, "ok");
    }
    Status NextRow(QueryState& qs, Row** r) override {
        Row* right_row;
        Status s = right_->NextRow(qs, &right_row);
        if (!s.Ok()) {
            {
                Status s = left_->NextRow(qs, &left_row_);
                if (!s.Ok())
                    return Status(false, "No more rows");
            }

            {
                right_->BeginScan(qs);
                Status s = right_->NextRow(qs, &right_row);
                if (!s.Ok())
                    return Status(false, "No more rows");
            } 
        }

        std::vector<Datum> result = left_row_->data_;
        result.insert(result.end(), right_row->data_.begin(), right_row->data_.end());
        *r = new Row(result);

        return Status(true, "ok");
    }
private:
    WorkTable* left_;
    WorkTable* right_;
    Row* left_row_;
};

class ConstantTable: public WorkTable {
public:
    ConstantTable(std::vector<Expr*> target_cols): target_cols_(target_cols), cur_(0) {}
    Status Analyze(QueryState& qs, WorkingAttributeSet** working_attrs) override {
        std::vector<std::string> names;
        std::vector<DatumType> types;
        for (Expr* e: target_cols_) {
            DatumType type;
            Status s = e->Analyze(qs, &type);      
            if (!s.Ok())
                return s;
            names.push_back("?col?");
            types.push_back(type);
        }

        *working_attrs = new WorkingAttributeSet("?table?", names, types);
        attrs_ = *working_attrs;

        return Status(true, "ok");
    }
    Status BeginScan(QueryState& qs) override {
        cur_ = 0;
        return Status(true, "ok");
    }
    Status NextRow(QueryState& qs, Row** r) override {
        if (cur_ > 0)
            return Status(false, "No more rows");

        std::vector<Datum> data;
        for (size_t i = 0; i < target_cols_.size(); i++) {
            data.emplace_back(0);
        }
        *r = new Row(data);
        cur_++;
        return Status(true, "ok");
    }
private:
    std::vector<Expr*> target_cols_;
    int cur_;
};

class PrimaryTable: public WorkTable {
public:
    PrimaryTable(Token tab_name, Token ref_name): tab_name_(tab_name.lexeme), ref_name_(ref_name.lexeme) {}
    //if an alias is not provided, the reference name is the same as the physical table name
    PrimaryTable(Token tab_name): tab_name_(tab_name.lexeme), ref_name_(tab_name.lexeme) {}

    Status Analyze(QueryState& qs, WorkingAttributeSet** working_attrs) override {
        std::string serialized_table;
        TableHandle catalogue = qs.storage->GetTable(qs.storage->CatalogueTableName());
        bool ok = catalogue.db->Get(rocksdb::ReadOptions(), tab_name_, &serialized_table).ok();

        if (!ok) {
            return Status(false, "Error: Table '" + tab_name_ + "' does not exist");
        }
        table_ = new Table(tab_name_, serialized_table);

        *working_attrs = new WorkingAttributeSet(table_, ref_name_);
        attrs_ = *working_attrs;

        return Status(true, "ok");
    }
    Status BeginScan(QueryState& qs) override {
        return table_->BeginScan(qs.storage, qs.scan_idx);
    }
    Status NextRow(QueryState& qs, Row** r) override {
        return table_->NextRow(qs.storage, r);
    }
private:
    std::string tab_name_;
    std::string ref_name_;
    Table* table_;
};

}

#pragma once

#include <string>
#include <iostream>
#include <algorithm>
#include "token.h"
#include "datum.h"
#include "row.h"
#include "table.h"
#include "status.h"
#include "db.h"

namespace wsldb {

//each query (and any subqueries nested inside) is associated with a single QueryState object
//this state holds the database handle, and also a stack of attributes for use during semantic analysis
//in case an inner subquery accesses a column in the outer query.  Similarly, a stack of the outer row(s)
//is stored during execution in case a subquery needs access to a column in the outer scope.
struct QueryState {
public:
    QueryState(DB* db): db(db) {
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
    DB* db;
    //state for aggregate functions
    Datum min_;
    Datum max_;
    Datum sum_;
    Datum count_;
    bool first_;
private:
    std::vector<WorkingAttributeSet*> analysis_scopes_;
    std::vector<Row*> scope_rows_;
};

//Putting class Stmt here since we need it in Expr,
//but putting it in stmt.h would cause a circular dependency
class Stmt {
public:
    Stmt(QueryState* qs): query_state_(qs) {}
    virtual Status Analyze(std::vector<TokenType>& types) = 0;
    virtual Status Execute() = 0;
    virtual std::string ToString() = 0;
    inline QueryState* GetQueryState() {
        return query_state_;
    }
private:
    QueryState* query_state_;
};


//QueryState is saved in both Expr and Stmt, could probably
//make a single class called QueryNode to store this, and have both Expr and Stmt inherit from that
class Expr {
public:
    Expr(QueryState* qs): query_state_(qs) {}
    virtual inline Status Eval(Row* r, Datum* result, bool* is_agg) = 0;
    virtual std::string ToString() = 0;
    virtual Status Analyze(QueryState* qs, TokenType* evaluated_type) = 0;
    inline QueryState* GetQueryState() {
        return query_state_;
    }
private:
    QueryState* query_state_;
};

class Literal: public Expr {
public:
    Literal(QueryState* qs, Token t): Expr(qs), t_(t) {}
    Literal(QueryState* qs, bool b): Expr(qs), t_(b ? Token("true", TokenType::TrueLiteral) : Token("false", TokenType::FalseLiteral)) {}
    Literal(QueryState* qs, int i): Expr(qs), t_(Token(std::to_string(i), TokenType::IntLiteral)) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = false;
        *result = Datum(t_);
        return Status(true, "ok");
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        *evaluated_type = Datum(t_).Type();
        return Status(true, "ok");
    }
private:
    Token t_;
};

class Binary: public Expr {
public:
    Binary(QueryState* qs, Token op, Expr* left, Expr* right):
        Expr(qs), op_(op), left_(left), right_(right) {}
    inline Status Eval(Row* row, Datum* result, bool* is_agg) override {
        *is_agg = false;

        Datum l;
        bool left_agg;
        Status s1 = left_->Eval(row, &l, &left_agg);
        if (!s1.Ok()) return s1;

        Datum r;
        bool right_agg;
        Status s2 = right_->Eval(row, &r, &right_agg);
        if (!s2.Ok()) return s2;

        if (l.Type() == TokenType::Null || r.Type() == TokenType::Null) {
            *result = Datum();
            return Status(true, "ok");
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

        return Status(true, "ok");
    }
    std::string ToString() override {
        return "(" + op_.lexeme + " " + left_->ToString() + " " + right_->ToString() + ")";
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        TokenType left_type;
        {
            Status s = left_->Analyze(qs, &left_type);
            if (!s.Ok())
                return s;
        }
        TokenType right_type;
        {
            Status s = right_->Analyze(qs, &right_type);
            if (!s.Ok())
                return s;
        }

        if (left_type == TokenType::Null || right_type == TokenType::Null) {
            *evaluated_type = TokenType::Null;
            return Status(true, "ok");
        }

        switch (op_.type) {
            case TokenType::Equal:
            case TokenType::NotEqual:
            case TokenType::Less:
            case TokenType::LessEqual:
            case TokenType::Greater:
            case TokenType::GreaterEqual:
                if (!(TokenTypeIsNumeric(left_type) && TokenTypeIsNumeric(right_type)) && left_type != right_type) {
                        return Status(false, "Error: Equality and relational operands must be same data types");
                }
                *evaluated_type = TokenType::Bool;
                break;
            case TokenType::Or:
            case TokenType::And:
                if (!(left_type == TokenType::Bool && right_type == TokenType::Bool)) {
                    return Status(false, "Error: Logical operator operands must be boolean types");
                }
                *evaluated_type = TokenType::Bool;
                break;
            case TokenType::Plus:
            case TokenType::Minus:
            case TokenType::Star:
            case TokenType::Slash:
                if (!(TokenTypeIsNumeric(left_type) && TokenTypeIsNumeric(right_type))) {
                    return Status(false, "Error: The '" + op_.lexeme + "' operator operands must both be a numeric type");
                } 
                *evaluated_type = left_type;
                break;
            default:
                return Status(false, "Implementation Error: op type not implemented in Binary expr!");
                break;
        }

        return Status(true, "ok");
    }
private:
    Token op_;
    Expr* left_;
    Expr* right_;
};

class Unary: public Expr {
public:
    Unary(QueryState* qs, Token op, Expr* right): Expr(qs), op_(op), right_(right) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = false;

        Datum right;
        bool inner_agg;
        Status s = right_->Eval(r, &right, &inner_agg);
        if (!s.Ok()) return s;

        switch (op_.type) {
            case TokenType::Minus:
                if (TokenTypeIsNumeric(right.Type())) {
                    *result = Datum(-WSLDB_NUMERIC_LITERAL(right));
                }
                break;
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
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        TokenType type;
        {
            Status s = right_->Analyze(qs, &type);
            if (!s.Ok())
                return s;
        }
        switch (op_.type) {
            case TokenType::Not:
                if (type != TokenType::Bool) {
                    return Status(false, "Error: 'not' operand must be a boolean type.");
                }
                *evaluated_type = TokenType::Bool;
                break;
            case TokenType::Minus:
                if (!TokenTypeIsNumeric(type)) {
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
    ColRef(QueryState* qs, Token t): Expr(qs), t_(t), table_ref_(""), idx_(-1), scope_(-1) {}
    ColRef(QueryState* qs, Token t, Token table_ref): Expr(qs), t_(t), table_ref_(table_ref.lexeme), idx_(-1), scope_(-1) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = false;
        *result = GetQueryState()->ScopeRowAt(scope_)->data_.at(idx_);
        return Status(true, "ok");
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        WorkingAttribute a;
        Status s = qs->GetWorkingAttribute(&a, table_ref_, t_.lexeme);
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
    ColAssign(QueryState* qs, Token col, Expr* right): 
        Expr(qs), col_(col), right_(right), scope_(-1), idx_(-1), physical_type_(TokenType::Null) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = false;

        Datum right;
        bool right_agg;
        Status s = right_->Eval(r, &right, &right_agg);
        if (!s.Ok()) return s;

        //demote int8 (working integer type in wsldb) evaluated type to fit physical type on disk
        if (right.Type() == TokenType::Int8 && physical_type_ == TokenType::Int4) {
            int32_t value = WSLDB_INTEGER_LITERAL(right);
            right = Datum(value);
        }

        r->data_.at(idx_) = right;

        *result = right; //result not used
        return Status(true, "ok");
    }
    std::string ToString() override {
        return "(:= " + col_.lexeme + " " + right_->ToString() + ")";
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        TokenType type;
        {
            Status s = right_->Analyze(qs, &type);
            if (!s.Ok())
                return s;
        }

        WorkingAttribute a;
        {
            std::string table_ref = "";
            Status s = qs->GetWorkingAttribute(&a, table_ref, col_.lexeme);
            if (!s.Ok())
                return s;
        }

        {
            Status s = a.CheckConstraints(type);
            if (!s.Ok())
                return s;
        }

        *evaluated_type = a.type;
        physical_type_ = a.type;
        idx_ = a.idx;
        scope_ = a.scope;

        return Status(true, "ok");
    }
private:
    Token col_;
    Expr* right_;
    int scope_;
    int idx_;
    //need physical type saved during analyze stage so that
    //the evaluation stage can demote integer working type (int8) to int4 if necessary
    TokenType physical_type_;
};

struct OrderCol {
    Expr* col;
    Expr* asc;
};

struct Call: public Expr {
public:
    Call(QueryState* qs, Token fcn, Expr* arg): Expr(qs), fcn_(fcn), arg_(arg) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = true;

        bool arg_agg;
        Datum arg;
        Status s = arg_->Eval(r, &arg, &arg_agg);
        if (!s.Ok())
            return s;

        QueryState* qs = GetQueryState();

        switch (fcn_.type) {
            case TokenType::Avg:
                qs->sum_ += arg;
                qs->count_ += Datum(1);
                *result = qs->sum_ / qs->count_;
                break;
            case TokenType::Count:
                qs->count_ += Datum(1);
                *result = qs->count_;
                break;
            case TokenType::Max:
                if (qs->first_ || arg > qs->max_) {
                    qs->max_ = arg;
                    qs->first_ = false;
                }
                *result = qs->max_;
                break;
            case TokenType::Min:
                if (qs->first_ || arg < qs->min_) {
                    qs->min_ = arg;
                    qs->first_ = false;
                }
                *result = qs->min_;
                break;
            case TokenType::Sum:
                qs->sum_ += arg;
                *result = qs->sum_;
                break;
            default:
                return Status(false, "Error: Invalid function name");
                break;
        }

        return Status(true, "ok");
    }
    std::string ToString() override {
        return fcn_.lexeme + arg_->ToString();
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        {
            Status s = arg_->Analyze(qs, evaluated_type);
            if (!s.Ok()) {
                return s;
            }
        }
        if (!TokenTypeIsAggregateFunction(fcn_.type)) {
            return Status(false, "Error: Function '" + fcn_.lexeme + "' does not exist");
        }
        return Status(true, "ok");
    }
private:
    Token fcn_;
    Expr* arg_;
};

class IsNull: public Expr {
public:
    IsNull(QueryState* qs, Expr* left): Expr(qs), left_(left) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = false;

        Datum d;
        bool left_agg;
        Status s = left_->Eval(r, &d, &left_agg);
        if (!s.Ok())
            return s;

        if (d.Type() == TokenType::Null) {
            *result = Datum(true);
        } else {
            *result = Datum(false);
        }

        return Status(true, "ok");
    }
    std::string ToString() override {
        return "IsNull";
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        *evaluated_type = TokenType::Bool;

        TokenType left_type;
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
    IsNotNull(QueryState* qs, Expr* left): Expr(qs), left_(left) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = false;

        Datum d;
        bool left_agg;
        Status s = left_->Eval(r, &d, &left_agg);
        if (!s.Ok())
            return s;

        if (d.Type() != TokenType::Null) {
            *result = Datum(true);
        } else {
            *result = Datum(false);
        }

        return Status(true, "ok");
    }
    std::string ToString() override {
        return "IsNotNull";
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        *evaluated_type = TokenType::Bool;

        TokenType left_type;
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
    ScalarSubquery(QueryState* qs, Stmt* stmt): Expr(qs), stmt_(stmt) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = false;

        Status s = stmt_->Execute();
        if (!s.Ok())
            return s;

        RowSet* rs = s.Tuples();
        if (rs->rows_.size() != 1)
            return Status(false, "Error: Subquery must produce a single row");

        if (rs->rows_.at(0)->data_.size() != 1)
            return Status(false, "Error: Subquery row must contain a single column");

        *result = rs->rows_.at(0)->data_.at(0);

        return Status(true, "ok");
    }
    std::string ToString() override {
        return "scalar subquery";
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        std::vector<TokenType> types;
        {
            Status s = stmt_->Analyze(types);
            if (!s.Ok())
                return s;
        }

        //TODO: this should be taken from GetQueryState()->attrs
        //at this point we don't know if the working table is a constant or physical or other table type
        //so how can we set *evaluated_type for type checking?
        if (types.size() != 1)
            return Status(false, "Error: Scalar subquery must return a single value");

        *evaluated_type = types.at(0);

        return Status(true, "ok");
    }
private:
    Stmt* stmt_;
};

class WorkTable {
public:
    virtual Status Analyze(QueryState* qs, WorkingAttributeSet** working_attrs) = 0;
    virtual Status BeginScan(DB* db) = 0;
    virtual Status NextRow(DB* db, Row** r) = 0;
};

class LeftJoin: public WorkTable {
public:
    LeftJoin(WorkTable* left, WorkTable* right, Expr* condition): left_(left), right_(right), condition_(condition) {}
    Status Analyze(QueryState* qs, WorkingAttributeSet** working_attrs) override {
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
            if (has_duplicate_tables)
                return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
        }

        qs->PushAnalysisScope(*working_attrs);
        {
            TokenType type;
            Status s = condition_->Analyze(qs, &type);
            if (!s.Ok())
                return s;

            if (type != TokenType::Bool) {
                return Status(false, "Error: Inner join condition must evaluate to a boolean type");
            }
        }
        qs->PopAnalysisScope();

        return Status(true, "ok");
    }
    Status BeginScan(DB* db) override {
        left_->BeginScan(db);
        right_->BeginScan(db);

        //initialize left row
        Status s = left_->NextRow(db, &left_row_);
        lefts_inserted_ = 0;
        if (!s.Ok())
            return Status(false, "No more rows");

        return Status(true, "ok");
    }
    Status NextRow(DB* db, Row** r) override {
        Row* right_row;

        while (true) {
            Status s = right_->NextRow(db, &right_row);
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

                    Status s = left_->NextRow(db, &left_row_);
                    lefts_inserted_ = 0;
                    if (!s.Ok())
                        return Status(false, "No more rows");
                }

                {
                    right_->BeginScan(db);
                    Status s = right_->NextRow(db, &right_row);
                    if (!s.Ok())
                        return Status(false, "No more rows");
                } 
            }

            {
                std::vector<Datum> result = left_row_->data_;
                result.insert(result.end(), right_row->data_.begin(), right_row->data_.end());

                *r = new Row(result);
                Datum d;
                bool is_agg;

                condition_->GetQueryState()->PushScopeRow(*r);
                Status s = condition_->Eval(*r, &d, &is_agg);
                condition_->GetQueryState()->PopScopeRow();

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

    Status Analyze(QueryState* qs, WorkingAttributeSet** working_attrs) override {
        Status s = right_join_->Analyze(qs, working_attrs);

        if (!s.Ok())
            return s;

        attr_count_ = (*working_attrs)->WorkingAttributeCount();

        return s;
    }
    Status BeginScan(DB* db) override {
        right_join_->BeginScan(db);
        do_right_ = true;

        return Status(true, "ok");
    }
    Status NextRow(DB* db, Row** r) override {
        //attempt grabbing row from right_join_
        //if none left, initialize left_ and right_, and start processing left join but only use values with nulled right sides
        if (do_right_) {
            Status s = right_join_->NextRow(db, r);
            if (s.Ok()) {
                return s;
            } else {
                //reset to begin augmented left outer join scan
                do_right_ = false;
                left_->BeginScan(db);
                right_->BeginScan(db);
                Status s = left_->NextRow(db, &left_row_);
                lefts_inserted_ = 0;
                if (!s.Ok())
                    return Status(false, "No more rows");
            }
        }
        

        //right join is done, so grabbing left join rows where the right side has no match, and will be nulled
        Row* right_row;

        while (true) {
            Status s = right_->NextRow(db, &right_row);
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
                    Status s = left_->NextRow(db, &left_row_);
                    lefts_inserted_ = 0;
                    if (!s.Ok())
                        return Status(false, "No more rows");
                }

                {
                    right_->BeginScan(db);
                    Status s = right_->NextRow(db, &right_row);
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
                bool is_agg;

                condition_->GetQueryState()->PushScopeRow(&temp);
                Status s = condition_->Eval(&temp, &d, &is_agg);
                condition_->GetQueryState()->PopScopeRow();

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
    Status Analyze(QueryState* qs, WorkingAttributeSet** working_attrs) override {
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
            if (has_duplicate_tables)
                return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
        }

        //Need to put current attributeset into QueryState temporarily so that Expr::Analyze
        //can use that data to perform semantic analysis for 'on' clause.  Normally Expr::Analyze is only
        //called once entire WorkTable is Analyzed an at least a single WorkingAttributeSet is in QueryState,
        //but InnerJoins have an Expr embedded as part of the WorkTable (the 'on' clause), so this is needed
        //Something similar is done in InnerJoin::NextRow
        qs->PushAnalysisScope(*working_attrs);
        {
            TokenType type;
            Status s = condition_->Analyze(qs, &type);
            if (!s.Ok())
                return s;

            if (type != TokenType::Bool) {
                return Status(false, "Error: Inner join condition must evaluate to a boolean type");
            }
        }
        qs->PopAnalysisScope();

        return Status(true, "ok");
    }
    Status BeginScan(DB* db) override {
        left_->BeginScan(db);
        right_->BeginScan(db);

        //initialize left row
        Status s = left_->NextRow(db, &left_row_);
        if (!s.Ok())
            return Status(false, "No more rows");

        return Status(true, "ok");
    }
    Status NextRow(DB* db, Row** r) override {
        Row* right_row;

        while (true) {
            Status s = right_->NextRow(db, &right_row);
            if (!s.Ok()) {
                //get new left
                {
                    Status s = left_->NextRow(db, &left_row_);
                    if (!s.Ok())
                        return Status(false, "No more rows");
                }

                {
                    right_->BeginScan(db);
                    Status s = right_->NextRow(db, &right_row);
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
                bool is_agg;

                //this is ugly, we need to do this since Expr::Eval is needed to generate a working table
                //something similar is done in InnerJoin::Analyze
                condition_->GetQueryState()->PushScopeRow(*r);
                Status s = condition_->Eval(*r, &d, &is_agg);
                condition_->GetQueryState()->PopScopeRow();

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
    Status Analyze(QueryState* qs, WorkingAttributeSet** working_attrs) override {
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
            if (has_duplicate_tables)
                return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
        }

        return Status(true, "ok");
    }
    Status BeginScan(DB* db) override {
        left_->BeginScan(db);
        right_->BeginScan(db);

        //initialize left row
        Status s = left_->NextRow(db, &left_row_);
        if (!s.Ok())
            return Status(false, "No more rows");

        return Status(true, "ok");
    }
    Status NextRow(DB* db, Row** r) override {
        Row* right_row;
        Status s = right_->NextRow(db, &right_row);
        if (!s.Ok()) {
            {
                Status s = left_->NextRow(db, &left_row_);
                if (!s.Ok())
                    return Status(false, "No more rows");
            }

            {
                right_->BeginScan(db);
                Status s = right_->NextRow(db, &right_row);
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
    Status Analyze(QueryState* qs, WorkingAttributeSet** working_attrs) override {
        std::vector<std::string> names;
        std::vector<TokenType> types;
        for (Expr* e: target_cols_) {
            TokenType type;
            Status s = e->Analyze(qs, &type);      
            if (!s.Ok())
                return s;
            names.push_back("?col?");
            types.push_back(type);
        }

        *working_attrs = new WorkingAttributeSet("?table?", names, types);

        return Status(true, "ok");
    }
    Status BeginScan(DB* db) override {
        cur_ = 0;
        return Status(true, "ok");
    }
    Status NextRow(DB* db, Row** r) override {
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

//TODO: should rename to PhysicalTable
class Physical: public WorkTable {
public:
    Physical(Token tab_name, Token ref_name): tab_name_(tab_name.lexeme), ref_name_(ref_name.lexeme) {}
    //if an alias is not provided, the reference name is the same as the physical table name
    Physical(Token tab_name): tab_name_(tab_name.lexeme), ref_name_(tab_name.lexeme) {}
    Status Analyze(QueryState* qs, WorkingAttributeSet** working_attrs) override {
        std::string serialized_table;
        if (!qs->db->GetSerializedTable(tab_name_, &serialized_table)) {
            return Status(false, "Error: Table '" + tab_name_ + "' does not exist");
        }
        table_ = new Table(tab_name_, serialized_table);

        db_handle_ = qs->db->GetIdxHandle(tab_name_);

        *working_attrs = new WorkingAttributeSet(table_, ref_name_);

        return Status(true, "ok");
    }
    Status BeginScan(DB* db) override {
        it_ = db_handle_->NewIterator(rocksdb::ReadOptions());
        it_->SeekToFirst();

        return Status(true, "ok");
    }
    Status NextRow(DB* db, Row** r) override {
        if (!it_->Valid()) return Status(false, "no more record");

        std::string value = it_->value().ToString();
        *r = new Row(table_->DeserializeData(value));
        it_->Next();

        return Status(true, "ok");
    }
private:
    std::string tab_name_;
    std::string ref_name_;
    rocksdb::DB* db_handle_;
    rocksdb::Iterator* it_;
public:
    Table* table_;
};

}

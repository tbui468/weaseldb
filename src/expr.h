#pragma once

#include <string>
#include <iostream>
#include <algorithm>
#include "token.h"
#include "datum.h"
#include "row.h"
#include "schema.h"
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

    inline AttributeSet* AnalysisScopesTop() const {
        return analysis_scopes_.back();
    }
    inline void PushAnalysisScope(AttributeSet* as) {
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

    Status GetAttribute(Attribute* attr, const std::string& table_ref_param, const std::string& col) const {
        for (int i = analysis_scopes_.size() - 1; i >= 0; i--) {
            Status s = GetAttributeAtScopeOffset(attr, table_ref_param, col, i);
            if (s.Ok() || i == 0)
                return s;
        }

        return Status(false, "should never reach this"); //keeping compiler quiet
    }
private:
    Status GetAttributeAtScopeOffset(Attribute* attr, const std::string& table_ref_param, const std::string& col, int i) const {
        std::string table_ref = table_ref_param;
        AttributeSet* as = analysis_scopes_.at(i);
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

        *attr = as->GetAttribute(table_ref, col);
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
    std::vector<AttributeSet*> analysis_scopes_;
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
                if (left_type != right_type) {
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
    Expr* left_;
    Expr* right_;
    Token op_;
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
                if (right.Type() == TokenType::Float4) {
                    *result = Datum(-right.AsFloat4());
                } else if (right.Type() == TokenType::Int4) {
                    *result = Datum(-right.AsInt4());
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
    Expr* right_;
    Token op_;
};


class ColRef: public Expr {
public:
    ColRef(QueryState* qs, Token t): Expr(qs), t_(t), idx_(-1), table_ref_(""), scope_(-1) {}
    ColRef(QueryState* qs, Token t, Token table_ref): Expr(qs), t_(t), idx_(-1), table_ref_(table_ref.lexeme), scope_(-1) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = false;
        *result = GetQueryState()->ScopeRowAt(scope_)->data_.at(idx_);
        return Status(true, "ok");
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        Attribute a;
        Status s = qs->GetAttribute(&a, table_ref_, t_.lexeme);
        if (!s.Ok())
            return s;

        idx_ = a.idx;
        *evaluated_type = a.type;
        scope_ = a.scope;

        return Status(true, "ok");
    }
private:
    Token t_;
    int idx_;
    int scope_;
    std::string table_ref_;
};


class ColAssign: public Expr {
public:
    ColAssign(QueryState* qs, Token col, Expr* right): Expr(qs), col_(col), right_(right), scope_(-1) {}
    inline Status Eval(Row* r, Datum* result, bool* is_agg) override {
        *is_agg = false;

        Datum right;
        bool right_agg;
        Status s = right_->Eval(r, &right, &right_agg);
        if (!s.Ok()) return s;

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

        Attribute a;
        std::string table_ref = "";
        Status s = qs->GetAttribute(&a, table_ref, col_.lexeme);
        if (!s.Ok())
            return s;

        idx_ = a.idx;
        *evaluated_type = a.type;
        scope_ = a.scope;

        return Status(true, "ok");
    }
private:
    Token col_;
    int idx_;
    int scope_;
    Expr* right_;
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
    virtual Status Analyze(QueryState* qs, AttributeSet** working_attrs) = 0;
    virtual Status BeginScan(DB* db) = 0;
    virtual Status NextRow(DB* db, Row** r) = 0;
    //DeletePrev, UpdatePrev, Insert are really strange in this case
    //since most WorkTables will not use them (only Physical Table will need it)
    virtual Status DeletePrev(DB* db) = 0;
    virtual Status UpdatePrev(DB* db, Row* r) = 0;
    virtual Status Insert(DB* db, const std::vector<Datum>& data) = 0;
    virtual std::string ToString() const = 0;
};

class InnerJoin: public WorkTable {
public:
    InnerJoin(WorkTable* left, WorkTable* right, Expr* condition): left_(left), right_(right), condition_(condition) {}
    Status Analyze(QueryState* qs, AttributeSet** working_attrs) override {
        AttributeSet* left_attrs;
        {
            Status s = left_->Analyze(qs, &left_attrs);
            if (!s.Ok())
                return s;
        }

        AttributeSet* right_attrs;
        {
            Status s = right_->Analyze(qs, &right_attrs);
            if (!s.Ok())
                return s;
        }

        {
            bool has_duplicate_tables;
            *working_attrs = new AttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
            if (has_duplicate_tables)
                return Status(false, "Error: Two tables cannot have the same name.  Use an alias to rename one or both tables");
        }

        //Need to put current attributeset into QueryState temporarily so that Expr::Analyze
        //can use that data to perform semantic analysis for 'on' clause.  Normally Expr::Analyze is only
        //called once entire WorkTable is Analyzed an at least a single AttributeSet is in QueryState,
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
    Status DeletePrev(DB* db) override {
        return Status(false, "Error: Cannot delete a row from a cross-joined table");
    }
    Status UpdatePrev(DB* db, Row* r) override {
        return Status(false, "Error: Cannot update a row in a cross-joined table");
    }
    Status Insert(DB* db, const std::vector<Datum>& data) override {
        return Status(false, "Error: Cannot insert value into a row in a cross-joined table");
    }
    std::string ToString() const override {
        return "inner join";
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
    Status Analyze(QueryState* qs, AttributeSet** working_attrs) override {
        AttributeSet* left_attrs;
        {
            Status s = left_->Analyze(qs, &left_attrs);
            if (!s.Ok())
                return s;
        }

        AttributeSet* right_attrs;
        {
            Status s = right_->Analyze(qs, &right_attrs);
            if (!s.Ok())
                return s;
        }

        {
            bool has_duplicate_tables;
            *working_attrs = new AttributeSet(left_attrs, right_attrs, &has_duplicate_tables);
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
    Status DeletePrev(DB* db) override {
        return Status(false, "Error: Cannot delete a row from a cross-joined table");
    }
    Status UpdatePrev(DB* db, Row* r) override {
        return Status(false, "Error: Cannot update a row in a cross-joined table");
    }
    Status Insert(DB* db, const std::vector<Datum>& data) override {
        return Status(false, "Error: Cannot insert value into a row in a cross-joined table");
    }
    std::string ToString() const override {
        return "cross join";
    }
private:
    Row* left_row_;
    WorkTable* left_;
    WorkTable* right_;
};

class ConstantTable: public WorkTable {
public:
    ConstantTable(std::vector<Expr*> target_cols): target_cols_(target_cols), cur_(0) {}
    Status Analyze(QueryState* qs, AttributeSet** working_attrs) override {
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

        *working_attrs = new AttributeSet("?table?", names, types);

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
        for (int i = 0; i < target_cols_.size(); i++) {
            data.emplace_back(0);
        }
        *r = new Row(data);
        cur_++;
        return Status(true, "ok");
    }
    Status DeletePrev(DB* db) override {
        return Status(false, "Error: Cannot delete a constant table row");
    }
    Status UpdatePrev(DB* db, Row* r) override {
        return Status(false, "Error: Cannot update a constant table row");
    }
    Status Insert(DB* db, const std::vector<Datum>& data) override {
        return Status(false, "Error: Cannot insert value into a constant table row");
    }
    std::string ToString() const override {
        return "constant table";
    }
private:
    int cur_;
    std::vector<Expr*> target_cols_;
};

//TODO: should rename to PhysicalTable
class Physical: public WorkTable {
public:
    Physical(Token tab_name, Token ref_name): tab_name_(tab_name.lexeme), ref_name_(ref_name.lexeme) {}
    //if an alias is not provided, the reference name is the same as the physical table name
    Physical(Token tab_name): tab_name_(tab_name.lexeme), ref_name_(tab_name.lexeme) {}
    Status Analyze(QueryState* qs, AttributeSet** working_attrs) override {
        std::string serialized_schema;
        if (!qs->db->TableSchema(tab_name_, &serialized_schema)) {
            return Status(false, "Error: Table '" + tab_name_ + "' does not exist");
        }
        schema_ = new Schema(tab_name_, serialized_schema);

        db_handle_ = qs->db->GetTableHandle(tab_name_);

        *working_attrs = new AttributeSet(schema_, ref_name_);

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
        *r = new Row(schema_->DeserializeData(value));
        it_->Next();

        return Status(true, "ok");
    }
    Status DeletePrev(DB* db) override {
        it_->Prev();
        std::string key = it_->key().ToString();
        db_handle_->Delete(rocksdb::WriteOptions(), key);
        it_->Next();
        return Status(true, "ok");
    }
    Status UpdatePrev(DB* db, Row* r) override {
        it_->Prev();
        std::string old_key = it_->key().ToString();
        std::string new_key = schema_->GetKeyFromData(r->data_);
        if (old_key.compare(new_key) != 0) {
            db_handle_->Delete(rocksdb::WriteOptions(), old_key);
        }
        db_handle_->Put(rocksdb::WriteOptions(), new_key, Datum::SerializeData(r->data_));
        it_->Next();
        return Status(true, "ok");
    }
    Status Insert(DB* db, const std::vector<Datum>& data) override {
        rocksdb::DB* tab_handle = db->GetTableHandle(tab_name_);
        std::string value = Datum::SerializeData(data);
        std::string key = schema_->GetKeyFromData(data);

        std::string test_value;
        rocksdb::Status status = tab_handle->Get(rocksdb::ReadOptions(), key, &test_value);
        if (status.ok()) {
            return Status(false, "Error: A record with the same primary key already exists");
        }

        tab_handle->Put(rocksdb::WriteOptions(), key, value);
        return Status(true, "ok");
    }
    std::string ToString() const override {
        return "physical table";
    }
private:
    std::string tab_name_;
    std::string ref_name_;
    rocksdb::DB* db_handle_;
    rocksdb::Iterator* it_;
public:
    Schema* schema_;
};

}

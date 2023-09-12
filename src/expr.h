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
    QueryState(DB* db): db(db) {}
    DB* db;
    std::vector<AttributeSet*> attrs;
    //std::vector<Row*> rows; //need to push rows onto stack when entering suqueries that access outer query data
    
public:
    inline void PushAttributeSet(AttributeSet* as) {
        attrs.push_back(as);
    }
    inline void PopAttributeSet() {
        attrs.pop_back();
    }
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


class Expr {
public:
    virtual Status Eval(RowSet* rs, std::vector<Datum>& output) = 0;
    virtual inline Status Eval(Row* r, Datum* result) = 0;
    virtual std::string ToString() = 0;
    virtual Status Analyze(QueryState* qs, TokenType* evaluated_type) = 0;
};

class Literal: public Expr {
public:
    Literal(Token t): t_(t) {}
    Literal(bool b): t_(b ? Token("true", TokenType::TrueLiteral) : Token("false", TokenType::FalseLiteral)) {}
    Literal(int i): t_(Token(std::to_string(i), TokenType::IntLiteral)) {}
    Status Eval(RowSet* rs, std::vector<Datum>& output) override {
        Datum d;
        for (Row* r: rs->rows_) {
            Status s = Eval(r, &d);
            if (!s.Ok()) return s;

            output.push_back(d);
        }
        return Status(true, "ok");
    }
    inline Status Eval(Row* r, Datum* result) override {
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
    Binary(Token op, Expr* left, Expr* right):
        op_(op), left_(left), right_(right) {}

    Status Eval(RowSet* rs, std::vector<Datum>& output) override {
        Datum d;
        for (Row* r: rs->rows_) {
            Status s = Eval(r, &d);
            if (!s.Ok()) return s;

            output.push_back(d);
        }

        return Status(true, "ok");
    }
    inline Status Eval(Row* row, Datum* result) override {
        Datum l;
        Status s1 = left_->Eval(row, &l);
        if (!s1.Ok()) return s1;
        Datum r;
        Status s2 = right_->Eval(row, &r);
        if (!s2.Ok()) return s2;

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
    Unary(Token op, Expr* right): op_(op), right_(right) {}
    Status Eval(RowSet* rs, std::vector<Datum>& output) override {
        Datum d;
        for (Row* r: rs->rows_) {
            Status s = Eval(r, &d);
            if (!s.Ok()) return s;

            output.push_back(d);
        }

        return Status(true, "ok");
    }
    inline Status Eval(Row* r, Datum* result) override {
        Datum right;
        Status s = right_->Eval(r, &right);
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
    ColRef(Token t): t_(t), idx_(-1), table_ref_("") {}
    ColRef(Token t, Token table_ref): t_(t), idx_(-1), table_ref_(table_ref.lexeme) {}
    Status Eval(RowSet* rs, std::vector<Datum>& output) override {
        Datum d;
        for (Row* r: rs->rows_) {
            Status s = Eval(r, &d);
            if (!s.Ok()) return s;

            output.push_back(d);
        }

        return Status(true, "ok");
    }
    inline Status Eval(Row* r, Datum* result) override {
        *result = r->data_.at(idx_);
        return Status(true, "ok");
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    Status Analyze(QueryState* qs, TokenType* evaluated_type) override {
        if (table_ref_ == "")  {//replace with default table name only if explicit table reference not provided
            //TODO: assuming reference is in top-most AttributeSet
            std::vector<std::string> tables = qs->attrs.back()->FindUniqueTablesWithCol(t_.lexeme);
            if (tables.size() > 1) {
                return Status(false, "Error: Column '" + t_.lexeme + "' can refer to columns in muliple tables.");
            } else if (tables.empty()) {
                return Status(false, "Error: Column '" + t_.lexeme + "' does not exist");
            } else {
                table_ref_ = tables.at(0);
            }
        }


        //TODO: assuming reference is in top-most AttributeSet
        if (!qs->attrs.back()->Contains(table_ref_, t_.lexeme)) {
            return Status(false, "Error: Column '" + t_.lexeme + "' does not exist");
        }

        //TODO: assuming reference is in top-most AttributeSet
        Attribute a = qs->attrs.back()->GetAttribute(table_ref_, t_.lexeme);
        idx_ = a.idx;
        *evaluated_type = a.type;

        return Status(true, "ok");
    }
private:
    Token t_;
    int idx_;
    std::string table_ref_;
};


class ColAssign: public Expr {
public:
    ColAssign(Token col, Expr* right): col_(col), right_(right) {}
    Status Eval(RowSet* rs, std::vector<Datum>& output) override {
        Datum d; //result of Eval(Row*) not used
        for (Row* r: rs->rows_) {
            Status s = Eval(r, &d);
            if (!s.Ok()) return s;
        } 

        return Status(true, "ok");
    }
    inline Status Eval(Row* r, Datum* result) override {
        Datum right;
        Status s = right_->Eval(r, &right);
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

        //TODO: assuming reference is in top-most AttributeSet
        std::vector<std::string> tables = qs->attrs.back()->FindUniqueTablesWithCol(col_.lexeme);
        if (tables.size() > 1) {
            return Status(false, "Error: Column '" + col_.lexeme + "' can refer to columns in muliple tables.");
        } else if (tables.empty()) {
            return Status(false, "Error: Column '" + col_.lexeme + "' does not exist");
        }

        std::string table_ref = tables.at(0);

        //TODO: assuming reference is in top-most AttributeSet
        if (!qs->attrs.back()->Contains(table_ref, col_.lexeme)) {
            return Status(false, "Error: Column '" + col_.lexeme + "' does not exist");
        }

        //TODO: assuming reference is in top-most AttributeSet
        Attribute a = qs->attrs.back()->GetAttribute(table_ref, col_.lexeme);
        idx_ = a.idx;
        *evaluated_type = a.type;

        return Status(true, "ok");
    }
private:
    Token col_;
    int idx_;
    Expr* right_;
};

struct OrderCol {
    Expr* col;
    Expr* asc;
};

struct Call: public Expr {
public:
    Call(Token fcn, Expr* arg): fcn_(fcn), arg_(arg) {}
    Status Eval(RowSet* rs, std::vector<Datum>& output) override {
        switch (fcn_.type) {
            case TokenType::Avg: {
                std::vector<Datum> values;
                arg_->Eval(rs, values);
                Datum s(0);
                for (Datum d: values) {
                    s = s + d;
                }
                output = { s / Datum(float(values.size())) };
                break;
            }
            case TokenType::Count: {
                output = { Datum(int(rs->rows_.size())) };
                break;
            }
            case TokenType::Max: {
                std::vector<Datum> values;
                arg_->Eval(rs, values);
                std::vector<Datum>::iterator it = std::max_element(values.begin(), values.end(), 
                        [](Datum& left, Datum& right) -> bool {
                            return left < right;
                        });
                output = {*it};
                break;
            }
            case TokenType::Min: {
                std::vector<Datum> values;
                arg_->Eval(rs, values);
                std::vector<Datum>::iterator it = std::min_element(values.begin(), values.end(), 
                        [](Datum& left, Datum& right) -> bool {
                            return left < right;
                        });
                output = {*it};
                break;
            }
            case TokenType::Sum: {
                std::vector<Datum> values;
                arg_->Eval(rs, values);
                Datum s(0);
                for (Datum d: values) {
                    s = s + d;
                }
                output = {s};
                break;
            }
            default:
                //Error should have been reported before getting to this point
                break;
        }
        return Status(true, "ok");
    }
    inline Status Eval(Row* r, Datum* result) override {
        return Status(false, "Error: Cannot call aggregate function on single row");
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

class ScalarSubquery: public Expr {
public:
    ScalarSubquery(Stmt* stmt): stmt_(stmt) {}
    Status Eval(RowSet* rs, std::vector<Datum>& output) override {
        //TODO: should run the subquery here rather than in Eval(Row*, Datum*)
        //if it's not a correlated subquery to avoid unecessary evaluation
        //The result can be cached and reused

        Datum d;
        for (Row* r: rs->rows_) {
            Status s = Eval(r, &d);
            if (!s.Ok()) return s;

            output.push_back(d);
        } 

        return Status(true, "ok");
    }
    inline Status Eval(Row* r, Datum* result) override {
        //run the stmt_ here if correlated subquery
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

        *working_attrs = AttributeSet::Concatenate(left_attrs, right_attrs);

        //Need to put current attributeset into QueryState temporarily so that Expr::Analyze
        //can use that data to perform semantic analysis for 'on' clause.  Normally Expr::Analyze is only
        //called once entire WorkTable is Analyzed an at least a single AttributeSet is in QueryState,
        //but InnerJoins have an Expr embedded as part of the WorkTable (the 'on' clause), so this is needed
        qs->PushAttributeSet(*working_attrs);
        {
            TokenType type;
            Status s = condition_->Analyze(qs, &type);
            if (!s.Ok())
                return s;

            if (type != TokenType::Bool) {
                return Status(false, "Error: Inner join condition must evaluate to a boolean type");
            }
        }
        qs->PopAttributeSet();

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
                Status s = condition_->Eval(*r, &d);
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

        *working_attrs = AttributeSet::Concatenate(left_attrs, right_attrs);

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

        *working_attrs = new AttributeSet();
        (*working_attrs)->AppendAttributes("?table?", names, types);

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

        *working_attrs = new AttributeSet();
        (*working_attrs)->AppendSchema(schema_, ref_name_);

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

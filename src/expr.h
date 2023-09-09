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

class Expr {
public:
    virtual Status Eval(RowSet* rs, std::vector<Datum>& output) = 0;
    virtual inline Status Eval(Row* r, Datum* result) = 0;
    virtual std::string ToString() = 0;
    virtual Status Analyze(const AttributeSet& as, TokenType* evaluated_type) = 0;
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
    Status Analyze(const AttributeSet& as, TokenType* evaluated_type) override {
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
    Status Analyze(const AttributeSet& as, TokenType* evaluated_type) override {
        TokenType left_type;
        {
            Status s = left_->Analyze(as, &left_type);
            if (!s.Ok())
                return s;
        }
        TokenType right_type;
        {
            Status s = right_->Analyze(as, &right_type);
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
    Status Analyze(const AttributeSet& as, TokenType* evaluated_type) override {
        TokenType type;
        {
            Status s = right_->Analyze(as, &type);
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
    Status Analyze(const AttributeSet& as, TokenType* evaluated_type) override {
        if (table_ref_ == "")  {//replace with default table name only if explicit table reference not provided
            std::vector<std::string> tables = as.FindUniqueTablesWithCol(t_.lexeme);
            if (tables.size() > 1) {
                return Status(false, "Error: Column '" + t_.lexeme + "' can refer to columns in muliple tables.");
            } else if (tables.empty()) {
                return Status(false, "Error: Column '" + t_.lexeme + "' does not exist");
            } else {
                table_ref_ = tables.at(0);
            }
        }

        if (!as.Contains(table_ref_, t_.lexeme)) {
            return Status(false, "Error: Column '" + t_.lexeme + "' does not exist");
        }

        Attribute a = as.GetAttribute(table_ref_, t_.lexeme);
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
    Status Analyze(const AttributeSet& as, TokenType* evaluated_type) override {
        TokenType type;
        {
            Status s = right_->Analyze(as, &type);
            if (!s.Ok())
                return s;
        }

        std::vector<std::string> tables = as.FindUniqueTablesWithCol(col_.lexeme);
        if (tables.size() > 1) {
            return Status(false, "Error: Column '" + col_.lexeme + "' can refer to columns in muliple tables.");
        } else if (tables.empty()) {
            return Status(false, "Error: Column '" + col_.lexeme + "' does not exist");
        }

        std::string table_ref = tables.at(0);

        if (!as.Contains(table_ref, col_.lexeme)) {
            return Status(false, "Error: Column '" + col_.lexeme + "' does not exist");
        }

        Attribute a = as.GetAttribute(table_ref, col_.lexeme);
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
    Status Analyze(const AttributeSet& as, TokenType* evaluated_type) override {
        {
            Status s = arg_->Analyze(as, evaluated_type);
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

class WorkTable {
public:
    virtual Status Analyze(DB* db) = 0;
    virtual Status BeginScan(DB* db) = 0;
    virtual Status NextRow(DB* db, Row** r) = 0;
    virtual Status DeletePrev(DB* db) = 0;
    virtual Status UpdatePrev(DB* db, Row* r) = 0;
    virtual Status EndScan(DB* db) = 0;
    virtual const AttributeSet& GetAttributes() const = 0;
    virtual Status Insert(DB* db, const std::vector<Datum>& data) = 0;
};

class ConstantTable: public WorkTable {
public:
    ConstantTable(int target_cols): target_cols_(target_cols), cur_(0) {}
    Status Analyze(DB* db) override {
        return Status(true, "ok");
    }
    Status BeginScan(DB* db) override {
        return Status(true, "ok");
    }
    Status NextRow(DB* db, Row** r) override {
        if (cur_ > 0)
            return Status(false, "No more rows");

        std::vector<Datum> data;
        for (int i = 0; i < target_cols_; i++) {
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
    Status EndScan(DB* db) override {
        return Status(true, "ok");
    }
    const AttributeSet& GetAttributes() const override {
        return attr_set_;
    }
    Status Insert(DB* db, const std::vector<Datum>& data) override {
        return Status(false, "Error: Cannot insert value into a constant table row");
    }
private:
    int cur_;
    AttributeSet attr_set_;
    int target_cols_;
};

//TODO: should rename to PhysicalTable
class Physical: public WorkTable {
public:
    Physical(Token t, Token alias): table_(t), alias_(alias.lexeme) {}
    Physical(Token t): table_(t), alias_(t.lexeme) {}
    Status Analyze(DB* db) override {
        std::string serialized_schema;
        if (!db->TableSchema(table_.lexeme, &serialized_schema)) {
            return Status(false, "Error: Table '" + table_.lexeme + "' does not exist");
        }
        schema_ = new Schema(table_.lexeme, serialized_schema);

        db_handle_ = db->GetTableHandle(table_.lexeme);

        attr_set_.AppendSchema(schema_, alias_);

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
    Status EndScan(DB* db) override {
        return Status(true, "ok");
    }
    const AttributeSet& GetAttributes() const override {
        return attr_set_;
    }
    Status Insert(DB* db, const std::vector<Datum>& data) override {
        rocksdb::DB* tab_handle = db->GetTableHandle(table_.lexeme);
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
private:
    Token table_;
    std::string alias_;
    rocksdb::DB* db_handle_;
    rocksdb::Iterator* it_;
public:
    AttributeSet attr_set_;
    Schema* schema_;
};

}

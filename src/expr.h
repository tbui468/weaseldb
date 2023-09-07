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
    virtual Status Analyze(Schema* schema, TokenType* evaluated_type) = 0;
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
    Status Analyze(Schema* schema, TokenType* evaluated_type) override {
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
    Status Analyze(Schema* schema, TokenType* evaluated_type) override {
        TokenType left_type;
        TokenType right_type;
        left_->Analyze(schema, &left_type);
        right_->Analyze(schema, &right_type);
        
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
    Status Analyze(Schema* schema, TokenType* evaluated_type) override {
        TokenType type;
        right_->Analyze(schema, &type);
        
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

class TableRef: public Expr {
public:
    TableRef(Token t): t_(t) {}
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
        *result = Datum(t_.lexeme);
        return Status(true, "ok");
    }
    std::string ToString() override {
        return "table ref";
    }
    Status Analyze(Schema* schema, TokenType* evaluated_type) {
        return Status(true, "ok");
    }
private:
    Token t_;
};

struct WorkTable {
    Expr* table; //either TableRef or Subquery
    Token alias;
};


class ColRef: public Expr {
public:
    ColRef(Token t): t_(t), idx_(-1), table_alias_("") {}
    ColRef(Token t, Token table_ref): t_(t), idx_(-1), table_alias_(table_ref.lexeme) {}
    Status Eval(RowSet* rs, std::vector<Datum>& output) override {
        if (rs->aliases_.find(table_alias_) == rs->aliases_.end()) {
            return Status(false, "Error: Table doesn't exist");
        }
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
    Status Analyze(Schema* schema, TokenType* evaluated_type) {
        idx_ = schema->GetFieldIdx(t_.lexeme);
        if (idx_ == -1) {
            return Status(false, "Error: Column '" + t_.lexeme + "' does not exist");
        }
        *evaluated_type = schema->Type(idx_);
        if (table_alias_ == "")  //replace with default table name only if alias not provided
            table_alias_ = schema->TableName();
        return Status(true, "ok");
    }
private:
    Token t_;
    int idx_;
    std::string table_alias_;
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
    Status Analyze(Schema* schema, TokenType* evaluated_type) {
        TokenType type;
        Status status = right_->Analyze(schema, &type);
        if (!status.Ok()) {
            return status;
        }

        idx_ = schema->GetFieldIdx(col_.lexeme);
        if (idx_ == -1) {
            return Status(false, "Error: Column '" + col_.lexeme + "' does not exist");
        }

        if (schema->Type(idx_) != type) {
            return Status(false, "Error: Value type does not match type in schema");
        }

        *evaluated_type = schema->Type(idx_);
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
    Status Analyze(Schema* schema, TokenType* evaluated_type) override {
        Status status = arg_->Analyze(schema, evaluated_type);
        if (!status.Ok()) {
            return status;
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

}

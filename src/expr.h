#pragma once

#include <string>
#include <iostream>
#include <algorithm>
#include "token.h"
#include "datum.h"
#include "tuple.h"
#include "schema.h"
#include "status.h"
#include "db.h"

namespace wsldb {

class Expr {
public:
    virtual std::vector<Datum> Eval(RowSet* rs) = 0;
    virtual std::string ToString() = 0;
    virtual Status Analyze(Schema* schema, TokenType* evaluated_type) = 0;
};

class Literal: public Expr {
public:
    Literal(Token t): t_(t) {}
    Literal(bool b): t_(b ? Token("true", TokenType::TrueLiteral) : Token("false", TokenType::FalseLiteral)) {}
    Literal(int i): t_(Token(std::to_string(i), TokenType::IntLiteral)) {}
    std::vector<Datum> Eval(RowSet* rs) override {
        std::vector<Datum> output;
        for (Row* r: rs->rows_) {
            output.emplace_back(t_);
        }
        return output;
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

    std::vector<Datum> Eval(RowSet* rs) override {
        std::vector<Datum> left_values = left_->Eval(rs);
        std::vector<Datum> right_values = right_->Eval(rs);

        std::vector<Datum> output;
        for (int i = 0; i < rs->rows_.size(); i++) {
            Datum l = left_values.at(i);
            Datum r = right_values.at(i);
            switch (op_.type) {
                case TokenType::Equal:
                    output.emplace_back(l.Compare(r).AsInt() == 0);
                    break;
                case TokenType::NotEqual:
                    output.emplace_back(l.Compare(r).AsInt() != 0);
                    break;
                case TokenType::Less:
                    output.emplace_back(l.Compare(r).AsInt() < 0);
                    break;
                case TokenType::LessEqual:
                    output.emplace_back(l.Compare(r).AsInt() <= 0);
                    break;
                case TokenType::Greater:
                    output.emplace_back(l.Compare(r).AsInt() > 0);
                    break;
                case TokenType::GreaterEqual:
                    output.emplace_back(l.Compare(r).AsInt() >= 0);
                    break;
                case TokenType::Plus:
                    output.emplace_back(l + r);
                    break;
                case TokenType::Minus:
                    output.emplace_back(l - r);
                    break;
                case TokenType::Star:
                    output.emplace_back(l * r);
                    break;
                case TokenType::Slash:
                    output.emplace_back(l / r);
                    break;
                case TokenType::Or:
                    output.emplace_back(l.AsBool() || r.AsBool());
                    break;
                case TokenType::And:
                    output.emplace_back(l.AsBool() && r.AsBool());
                    break;
                default:
                    std::cout << "invalid op\n";
                    return {}; //keep compiler quiet
            }
        }

        return output;
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
    std::vector<Datum> Eval(RowSet* rs) override {
        std::vector<Datum> right_values = right_->Eval(rs);

        std::vector<Datum> output;
        for (Datum right: right_values) {
            switch (op_.type) {
                case TokenType::Minus:
                    if (right.Type() == TokenType::Float4) {
                        output.emplace_back(-right.AsFloat4());
                    } else if (right.Type() == TokenType::Int) {
                        output.emplace_back(-right.AsInt());
                    }
                    break;
                case TokenType::Not:
                    output.emplace_back(!right.AsBool());
                    break;
                default:
                    std::cout << "invalid op\n";
                    return {}; //keep compiler quiet
            }
        }

        return output;
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

class ColRef: public Expr {
public:
    ColRef(Token t): t_(t), idx_(-1) {}
    std::vector<Datum> Eval(RowSet* rs) override {
        std::vector<Datum> output;
        for (Row* r: rs->rows_) {
            output.push_back(r->data_.at(idx_));
        }

        return output;
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
        return Status(true, "ok");
    }
    int ColIdx() {
        return idx_;
    }
private:
    Token t_;
    int idx_;
};

class AssignCol: public Expr {
public:
    AssignCol(Token col, Expr* right): col_(col), right_(right) {}
    std::vector<Datum> Eval(RowSet* rs) override {
        std::vector<Datum> right_values = right_->Eval(rs);
        
        for (int i = 0; i < rs->rows_.size(); i++) {
            rs->rows_.at(i)->data_.at(idx_) = right_values.at(i);
        }
        return {};
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
    std::vector<Datum> Eval(RowSet* rs) override {
        switch (fcn_.type) {
            case TokenType::Avg:
                //TODO: implement avg
                break;
            case TokenType::Count:
                return { Datum(int(rs->rows_.size())) };
            case TokenType::Max: {
                std::vector<Datum> values = arg_->Eval(rs);
                std::vector<Datum>::iterator it = std::max_element(values.begin(), values.end(), 
                        [](Datum& left, Datum& right) -> bool {
                            return left.Compare(right).AsInt() < 0;
                        });
                return {*it};
            }
            case TokenType::Min: {
                std::vector<Datum> values = arg_->Eval(rs);
                std::vector<Datum>::iterator it = std::min_element(values.begin(), values.end(), 
                        [](Datum& left, Datum& right) -> bool {
                            return left.Compare(right).AsInt() < 0;
                        });
                return {*it};
            }
            case TokenType::Sum: {
                std::vector<Datum> values = arg_->Eval(rs);
                Datum s(0);
                for (Datum d: values) {
                    s = s + d;
                }
                return {s};
            }
            default:
                //Error should have been reported before getting to this point
                break;
        }
        return {};
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

class TableRef: public Expr {
public:
    TableRef(Token t): t_(t) {}
    std::vector<Datum> Eval(RowSet* rs) override {
        std::vector<Datum> output;

        for (Row* r: rs->rows_) {
            output.emplace_back(t_.lexeme);
        }

        return output;
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


}

#pragma once

#include <string>
#include <iostream>
#include <algorithm>
#include "token.h"
#include "datum.h"
#include "tuple.h"
#include "schema.h"
#include "status.h"

namespace wsldb {


class Expr {
public:
    virtual Datum Eval(Row* tuple) = 0;
    virtual std::string ToString() = 0;
    virtual Status Analyze(Schema* schema, TokenType* evaluated_type) = 0;
    virtual bool ContainsAggregateFunctions() = 0;
    virtual bool ContainsNonAggregatedColRef(const std::vector<int>& nongroup_cols) = 0;
};

class Literal: public Expr {
public:
    Literal(Token t): t_(t) {}
    Literal(bool b): t_(b ? Token("true", TokenType::TrueLiteral) : Token("false", TokenType::FalseLiteral)) {}
    Literal(int i): t_(Token(std::to_string(i), TokenType::IntLiteral)) {}
    Datum Eval(Row* tuple) override {
        return Datum(t_);
    }
    std::string ToString() override {
        return t_.lexeme;
    }
    Status Analyze(Schema* schema, TokenType* evaluated_type) override {
        *evaluated_type = Datum(t_).Type();
        return Status(true, "ok");
    }
    bool ContainsAggregateFunctions() override {
        return false;
    }
    bool ContainsNonAggregatedColRef(const std::vector<int>& nongroup_cols) override {
        return false;
    }
private:
    Token t_;
};

class Binary: public Expr {
public:
    Binary(Token op, Expr* left, Expr* right):
        op_(op), left_(left), right_(right) {}

    Datum Eval(Row* tuple) override {
        Datum leftd = left_->Eval(tuple);
        Datum rightd = right_->Eval(tuple);
        switch (op_.type) {
            case TokenType::Equal:
                return Datum(leftd.Compare(rightd).AsInt() == 0);
            case TokenType::NotEqual:
                return Datum(leftd.Compare(rightd).AsInt() != 0);
            case TokenType::Less:
                return Datum(leftd.Compare(rightd).AsInt() < 0);
            case TokenType::LessEqual:
                return Datum(leftd.Compare(rightd).AsInt() <= 0);
            case TokenType::Greater:
                return Datum(leftd.Compare(rightd).AsInt() > 0);
            case TokenType::GreaterEqual:
                return Datum(leftd.Compare(rightd).AsInt() >= 0);
            case TokenType::Plus:
                return Datum(leftd.AsInt() + rightd.AsInt());
            case TokenType::Minus:
                return Datum(leftd.AsInt() - rightd.AsInt());
            case TokenType::Star:
                return Datum(leftd.AsInt() * rightd.AsInt());
            case TokenType::Slash:
                return Datum(leftd.AsInt() / rightd.AsInt());
            case TokenType::Or:
                return Datum(leftd.AsBool() || rightd.AsBool());
            case TokenType::And:
                return Datum(leftd.AsBool() && rightd.AsBool());
            default:
                std::cout << "invalid op\n";
                return Datum(false); //keep compiler quiet
        }
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

    bool ContainsAggregateFunctions() override {
        return left_->ContainsAggregateFunctions() || right_->ContainsAggregateFunctions();
    }
    bool ContainsNonAggregatedColRef(const std::vector<int>& nongroup_cols) override {
        return left_->ContainsNonAggregatedColRef(nongroup_cols) || right_->ContainsNonAggregatedColRef(nongroup_cols);
    }
private:
    Expr* left_;
    Expr* right_;
    Token op_;
};

class Unary: public Expr {
public:
    Unary(Token op, Expr* right): op_(op), right_(right) {}
    Datum Eval(Row* tuple) override {
        Datum right = right_->Eval(tuple);
        switch (op_.type) {
            case TokenType::Minus:
                return Datum(-right.AsInt());
            case TokenType::Not:
                return Datum(!right.AsBool());
            default:
                std::cout << "invalid op\n";
                return Datum(false); //keep compiler quiet
        }
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
    bool ContainsAggregateFunctions() override {
        return right_->ContainsAggregateFunctions();
    }
    bool ContainsNonAggregatedColRef(const std::vector<int>& nongroup_cols) override {
        return right_->ContainsNonAggregatedColRef(nongroup_cols);
    }
private:
    Expr* right_;
    Token op_;
};

class ColRef: public Expr {
public:
    ColRef(Token t): t_(t), idx_(-1) {}
    Datum Eval(Row* tuple) override {
        return tuple->GetCol(idx_);
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
    bool ContainsAggregateFunctions() override {
        return false;
    }
    bool ContainsNonAggregatedColRef(const std::vector<int>& nongroup_cols) override {
        if (std::find(nongroup_cols.begin(), nongroup_cols.end(), idx_) != nongroup_cols.end()) {
            return true;
        }

        return false;
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
    Datum Eval(Row* tuple) override {
        tuple->SetCol(idx_, right_->Eval(tuple));
        return Datum(0);
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
    bool ContainsAggregateFunctions() override {
        return right_->ContainsAggregateFunctions();
    }
    bool ContainsNonAggregatedColRef(const std::vector<int>& nongroup_cols) override {
        return false; //TODO: need to think this through - could this ever be true?
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
    Datum Eval(Row* row) override {
        int col_idx = ((ColRef*)arg_)->ColIdx(); //ugly, but it gets the job done
        std::vector<Datum> cols = row->GetCols(col_idx);
        switch (fcn_.type) {
            case TokenType::Avg:
                //TODO:
                break;
            case TokenType::Count: {
                return Datum(int(cols.size()));
            }
            case TokenType::Max: {
                return *std::max_element(cols.begin(), cols.end(), 
                        [](Datum left, Datum right) -> bool {
                            return left.Compare(right).AsInt() < 0;
                        });
            }
            case TokenType::Min: {
                return *std::min_element(cols.begin(), cols.end(), 
                        [](Datum left, Datum right) -> bool {
                            return left.Compare(right).AsInt() < 0;
                        });
            }
            case TokenType::Sum:
                //TODO
                break;
            default:
                //Error should have been reported before getting to this point
                break;
        }
        return Datum(0); //silence warnings for now
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
    bool ContainsAggregateFunctions() override {
        return true;
    }
    bool ContainsNonAggregatedColRef(const std::vector<int>& nongroup_cols) override {
        return false;
    }
private:
    Token fcn_;
    Expr* arg_;
};

}

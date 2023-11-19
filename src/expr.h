#pragma once

#include <string>

#include "token.h"
#include "datum.h"
#include "row.h"
#include "schema.h"
#include "status.h"
#include "iterator.h"

namespace wsldb {


class Stmt;

enum class ExprType {
    Literal,
    Binary,
    Unary,
    ColRef,
    ColAssign,
    Call,
    IsNull,
    ScalarSubquery,
    Predict,
    Cast
};

class Expr {
public:
    virtual std::string ToString() = 0;
    virtual ExprType Type() const = 0;
};

class Literal: public Expr {
public:
    Literal(Token t): t_(t) {}
    Literal(bool b): t_(b ? Token("true", TokenType::TrueLiteral) : Token("false", TokenType::FalseLiteral)) {}
    Literal(int i): t_(Token(std::to_string(i), TokenType::IntLiteral)) {}
    std::string ToString() override {
        return t_.lexeme;
    }
    ExprType Type() const override {
        return ExprType::Literal;
    }
public:
    Token t_;
};

class Binary: public Expr {
public:
    Binary(Token op, Expr* left, Expr* right):
        op_(op), left_(left), right_(right) {}
    std::string ToString() override {
        return "(" + op_.lexeme + " " + left_->ToString() + " " + right_->ToString() + ")";
    }
    ExprType Type() const override {
        return ExprType::Binary;
    }
public:
    Token op_;
    Expr* left_;
    Expr* right_;
};

class Unary: public Expr {
public:
    Unary(Token op, Expr* right): op_(op), right_(right) {}
    std::string ToString() override {
        return "(" + op_.lexeme + " " + right_->ToString() + ")";
    }
    ExprType Type() const override {
        return ExprType::Unary;
    }
public:
    Token op_;
    Expr* right_;
};


class ColRef: public Expr {
public:
    ColRef(Token t): t_(t), table_ref_(""), idx_(-1), scope_(-1) {}
    ColRef(Token t, Token table_ref): t_(t), table_ref_(table_ref.lexeme), idx_(-1), scope_(-1) {}
    std::string ToString() override {
        return t_.lexeme;
    }
    ExprType Type() const override {
        return ExprType::ColRef;
    }
public:
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
    std::string ToString() override {
        return "(:= " + col_.lexeme + " " + right_->ToString() + ")";
    }
    ExprType Type() const override {
        return ExprType::ColAssign;
    }
public:
    Token col_;
    Expr* right_;
    int scope_;
    int idx_;
    DatumType field_type_ {DatumType::Null};
};

struct OrderCol {
    Expr* col;
    Expr* asc;
};

struct Call: public Expr {
public:
    Call(Token fcn, Expr* arg): fcn_(fcn), arg_(arg) {}
    std::string ToString() override {
        return fcn_.lexeme + arg_->ToString();
    }
    ExprType Type() const override {
        return ExprType::Call;
    }
public:
    Token fcn_;
    Expr* arg_;
};

class IsNull: public Expr {
public:
    IsNull(Expr* left): left_(left) {}
    std::string ToString() override {
        return "IsNull";
    }
    ExprType Type() const override {
        return ExprType::IsNull;
    }
public:
    Expr* left_;
};

class ScalarSubquery: public Expr {
public:
    ScalarSubquery(Stmt* stmt): stmt_(stmt) {}
    std::string ToString() override {
        return "scalar subquery";
    }
    ExprType Type() const override {
        return ExprType::ScalarSubquery;
    }
public:
    Stmt* stmt_;
};

class Predict: public Expr {
public:
    Predict(Token model_name, Expr* arg): model_name_(model_name), arg_(arg) {}
    std::string ToString() override {
        return "predict";
    }
    ExprType Type() const override {
        return ExprType::Predict;
    }
public:
    Token model_name_;
    Expr* arg_;
};

class Cast: public Expr {
public:
    Cast(Expr* value, Token type): value_(value), type_(type) {}
    std::string ToString() override {
        return "cast";
    }
    ExprType Type() const override {
        return ExprType::Cast;
    }
public:
    Expr* value_;
    Token type_;
};

enum class ScanType {
    Left, //right-join is just a left-join with the input scans swapped
    Full,
    Inner,
    Constant,
    Table,
    Select,
    Product
};

class Scan {
public:
    std::vector<Attribute> GetAttributes() const {
        return attrs_->GetAttributes();
    }
    virtual ScanType Type() const = 0;
public:
    AttributeSet* attrs_ { nullptr };
};

class LeftJoin: public Scan {
public:
    LeftJoin(Scan* left, Scan* right, Expr* condition): left_(left), right_(right), condition_(condition) {}
    ScanType Type() const override {
        return ScanType::Left;
    }
public:
    //used to track number of times a given left row is inserted into the final working table
    //if a row is inserted 0 times, then insert the left row and fill in the right row with nulls
    int lefts_inserted_;
    int right_attr_count_;
    Row* left_row_;
    Scan* left_;
    Scan* right_;
    Expr* condition_;
};

//Idea: make a left join, run that to get left rows + rows in common
//Then run the right join, but only include rights with left sides that are nulled out
class FullJoin: public Scan {
public:
    FullJoin(Scan* left, Scan* right, Expr* condition): 
        left_(left), right_(right), condition_(condition), right_join_(new LeftJoin(right, left, condition)) {}
    ScanType Type() const override {
        return ScanType::Full;
    }
public:
    //used to track number of times a given left row is inserted into the final working table
    //if a row is inserted 0 times, then insert the left row and fill in the right row with nulls
    int lefts_inserted_;
    int attr_count_;
    bool do_right_;
    Row* left_row_;
    Scan* left_;
    Scan* right_;
    Expr* condition_;
    LeftJoin* right_join_;
};



class InnerJoin: public Scan {
public:
    InnerJoin(Scan* left, Scan* right, Expr* condition): left_(left), right_(right), condition_(condition) {}
    ScanType Type() const override {
        return ScanType::Inner;
    }
public:
    Row* left_row_;
    Scan* left_;
    Scan* right_;
    Expr* condition_;
};

class ConstantTable: public Scan {
public:
    ConstantTable(std::vector<Expr*> target_cols): target_cols_(target_cols), cur_(0) {}
    ScanType Type() const override {
        return ScanType::Constant;
    }
public:
    std::vector<Expr*> target_cols_;
    int cur_;
};

class PrimaryTable: public Scan {
public:
    PrimaryTable(Token tab_name, Token ref_name): tab_name_(tab_name.lexeme), ref_name_(ref_name.lexeme) {}
    //if an alias is not provided, the reference name is the same as the physical table name
    PrimaryTable(Token tab_name): tab_name_(tab_name.lexeme), ref_name_(tab_name.lexeme) {}
    ScanType Type() const override {
        return ScanType::Table;
    }
public:
    std::string tab_name_;
    std::string ref_name_;
    Iterator* it_;
    int scan_idx_; //index to scan (default is 0, which is the primary index)
    Schema* schema_;
};

class SelectScan: public Scan {
public:
    SelectScan(Scan* scan, Expr* expr): scan_(scan), expr_(expr) {}
    ScanType Type() const override {
        return ScanType::Select;
    }
public:
    Scan* scan_;
    Expr* expr_;
};

class ProductScan: public Scan {
public:
    ProductScan(Scan* left, Scan* right): left_(left), right_(right), left_row_(nullptr) {}
    ScanType Type() const override {
        return ScanType::Product;
    }
public:
    Scan* left_;
    Scan* right_;
    Row* left_row_;
};

}

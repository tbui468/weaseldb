#pragma once

#include <string>

#include "token.h"
#include "datum.h"
#include "row.h"
#include "table.h"
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
    virtual void Reset() = 0;
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
    void Reset() override {
    }
public:
    Token t_;
};

class Binary: public Expr {
public:
    Binary(Token op, Expr* left, Expr* right):
        op_(op), left_(left), right_(right) {}
    std::string ToString() override {
        return left_->ToString() + op_.lexeme + right_->ToString();
    }
    ExprType Type() const override {
        return ExprType::Binary;
    }
    void Reset() override {
        left_->Reset();
        right_->Reset();
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
        return op_.lexeme + right_->ToString();
    }
    ExprType Type() const override {
        return ExprType::Unary;
    }
    void Reset() override {
        right_->Reset();
    }
public:
    Token op_;
    Expr* right_;
};

struct Column {
    std::string table;
    std::string name;
};


class ColRef: public Expr {
public:
    ColRef(Column col): col_(col) {}
    std::string ToString() override {
        return col_.table + "." + col_.name;
    }
    ExprType Type() const override {
        return ExprType::ColRef;
    }
    void Reset() override {
    }
public:
    Column col_;
};


//Used for both InsertStmt and UpdateStmt
class ColAssign: public Expr {
public:
    ColAssign(Column col, Expr* right): col_(col), right_(right) {}
    std::string ToString() override {
        return col_.table + ". " + col_.name + "=" + right_->ToString();
    }
    ExprType Type() const override {
        return ExprType::ColAssign;
    }
    void Reset() override {
        right_->Reset();
    }
public:
    Column col_;
    Expr* right_;
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
        return fcn_.lexeme + "(" + arg_->ToString() + ")";
    }
    ExprType Type() const override {
        return ExprType::Call;
    }
    void Reset() override {
        arg_->Reset();
        min_ = Datum();
        max_ = Datum();
        sum_ = Datum(0);
        count_ = Datum(0);
        first_ = true;
    }
public:
    Token fcn_;
    Expr* arg_;

    Datum min_ {Datum()};
    Datum max_ {Datum()};
    Datum sum_ {Datum(0)};
    Datum count_ {Datum(0)};
    bool first_ {true};
};

class IsNull: public Expr {
public:
    IsNull(Expr* left): left_(left) {}
    std::string ToString() override {
        return left_->ToString() + " is null";
    }
    ExprType Type() const override {
        return ExprType::IsNull;
    }
    void Reset() override {
        left_->Reset();
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
    void Reset() override {
    }
public:
    Stmt* stmt_;
};

class Predict: public Expr {
public:
    Predict(Token model_name, Expr* arg): model_name_(model_name), arg_(arg) {}
    std::string ToString() override {
        return model_name_.lexeme + "(" + arg_->ToString() + ")";
    }
    ExprType Type() const override {
        return ExprType::Predict;
    }
    void Reset() override {
        arg_->Reset();
    }
public:
    Token model_name_;
    Expr* arg_;
};

class Cast: public Expr {
public:
    Cast(Expr* value, Token type): value_(value), type_(type) {}
    std::string ToString() override {
        return "cast(" + value_->ToString() + " as " + type_.lexeme + ")";
    }
    ExprType Type() const override {
        return ExprType::Cast;
    }
    void Reset() override {
        value_->Reset();
    }
public:
    Expr* value_;
    Token type_;
};

enum class ScanType {
    Constant,
    Table,
    Select,
    Product,
    OuterSelect,
    Project
};

class Scan {
public:
    virtual ScanType Type() const = 0;
    virtual bool IsUpdatable() const = 0;
public:
    AttributeSet* output_attrs_ { nullptr };
};

class ConstantScan: public Scan {
public:
    ConstantScan(std::vector<Expr*> target_cols): target_cols_(target_cols), cur_(0) {}
    ScanType Type() const override {
        return ScanType::Constant;
    }
    bool IsUpdatable() const override {
        return false;
    }
public:
    std::vector<Expr*> target_cols_;
    int cur_;
};

class TableScan: public Scan {
public:
    TableScan(Token tab_name, Token ref_name): tab_name_(tab_name.lexeme), ref_name_(ref_name.lexeme) {}
    //if an alias is not provided, the reference name is the same as the physical table name
    TableScan(Token tab_name): tab_name_(tab_name.lexeme), ref_name_(tab_name.lexeme) {}
    ScanType Type() const override {
        return ScanType::Table;
    }
    bool IsUpdatable() const override {
        return true;
    }
public:
    std::string tab_name_;
    std::string ref_name_;
    Iterator* it_;
    Table* table_;
    int scan_idx_ {0};
};

class SelectScan: public Scan {
public:
    SelectScan(Scan* scan, Expr* expr): scan_(scan), expr_(expr) {}
    ScanType Type() const override {
        return ScanType::Select;
    }
    bool IsUpdatable() const override {
        return scan_->IsUpdatable();
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
    bool IsUpdatable() const override {
        return false;
    }
public:
    Scan* left_;
    Scan* right_;
    Row* left_row_;
};

class OuterSelectScan: public Scan {
public:
    OuterSelectScan(ProductScan* scan, Expr* expr, bool include_left, bool include_right): 
        scan_(scan), expr_(expr), include_left_(include_left), include_right_(include_right), scanning_rows_(true) {}

    ScanType Type() const override {
        return ScanType::OuterSelect;
    }
    bool IsUpdatable() const override {
        return false;
    }
public:
    ProductScan* scan_;
    Expr* expr_;
    bool include_left_;
    bool include_right_;
    std::unordered_map<std::string, bool> left_pass_table_;
    std::unordered_map<std::string, bool> right_pass_table_;
    std::unordered_map<std::string, bool>::iterator left_it_;
    std::unordered_map<std::string, bool>::iterator right_it_;
    bool scanning_rows_;
};


class ProjectScan: public Scan {
public:
    ProjectScan(Scan* input, 
                std::vector<Expr*> projs, 
                std::vector<Expr*> group_cols, 
                Expr* having_clause,
                std::vector<OrderCol> order_cols, 
                Expr* limit, 
                bool distinct):
                    input_(input), 
                    projs_(std::move(projs)), 
                    group_cols_(std::move(group_cols)), 
                    having_clause_(having_clause), 
                    order_cols_(std::move(order_cols)), 
                    limit_(limit), 
                    distinct_(distinct) {}
    ScanType Type() const override {
        return ScanType::Project;
    }
    bool IsUpdatable() const override {
        return false;
    }
    std::vector<Attribute> OutputAttributes() const {
        std::vector<Attribute> attrs = output_attrs_->GetAttributes();
        int new_size = attrs.size() - ghost_column_count_;
        attrs.resize(new_size);
        return attrs;
    }
public:
    Scan* input_;
    std::vector<Expr*> projs_;
    std::vector<Expr*> group_cols_;
    Expr* having_clause_;
    std::vector<OrderCol> order_cols_;
    Expr* limit_;
    bool distinct_;
    AttributeSet* input_attrs_ {nullptr};
    RowSet* output_;
    size_t cursor_ {0};
    int ghost_column_count_ {0};
};

}

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
    virtual StmtType Type() const = 0;
};

enum class ExprType {
    Literal,
    Binary,
    Unary,
    ColRef,
    ColAssign,
    Call,
    IsNull,
    ScalarSubquery
};

//QueryState is saved in both Expr and Stmt, could probably
//make a single class called QueryNode to store this, and have both Expr and Stmt inherit from that
class Expr {
public:
    //TODO: reuslt and is_agg can be put inside of query state
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

enum class ScanType {
    Left, //right-join is just a left-join with the input scans swapped
    Full,
    Inner,
    Cross,
    Constant,
    Table
};

class WorkTable {
public:
//    virtual Status BeginScan(QueryState& qs) = 0;
//    virtual Status NextRow(QueryState& qs, Row** r) = 0;
    std::vector<Attribute> GetAttributes() const {
        return attrs_->GetAttributes();
    }
    virtual ScanType Type() const = 0;
public:
    WorkingAttributeSet* attrs_ { nullptr };
};

class LeftJoin: public WorkTable {
public:
    LeftJoin(WorkTable* left, WorkTable* right, Expr* condition): left_(left), right_(right), condition_(condition) {}
    ScanType Type() const override {
        return ScanType::Left;
    }
public:
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
    WorkTable* left_;
    WorkTable* right_;
    Expr* condition_;
    LeftJoin* right_join_;
};



class InnerJoin: public WorkTable {
public:
    InnerJoin(WorkTable* left, WorkTable* right, Expr* condition): left_(left), right_(right), condition_(condition) {}
    ScanType Type() const override {
        return ScanType::Inner;
    }
public:
    Row* left_row_;
    WorkTable* left_;
    WorkTable* right_;
    Expr* condition_;
};

class CrossJoin: public WorkTable {
public:
    CrossJoin(WorkTable* left, WorkTable* right): left_(left), right_(right), left_row_(nullptr) {}
    ScanType Type() const override {
        return ScanType::Cross;
    }
public:
    WorkTable* left_;
    WorkTable* right_;
    Row* left_row_;
};

class ConstantTable: public WorkTable {
public:
    ConstantTable(std::vector<Expr*> target_cols): target_cols_(target_cols), cur_(0) {}
    ScanType Type() const override {
        return ScanType::Constant;
    }
public:
    std::vector<Expr*> target_cols_;
    int cur_;
};

class PrimaryTable: public WorkTable {
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
    Table* table_;
};

}

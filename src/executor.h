#pragma once

#include "status.h"
#include "expr.h"
#include "stmt.h"
#include "storage.h"
#include "inference.h"

namespace wsldb {


class Executor {
public:
    Executor(Storage* storage, Inference* inference, Txn** txn): storage_(storage), inference_(inference), txn_(txn) {
        ResetAggState();
    }
    std::vector<Status> ExecuteQuery(const std::string& query);
private:
    Status Execute(Stmt* stmt);
    //statements
    Status CreateExecutor(CreateStmt* stmt);
    Status InsertExecutor(InsertStmt* stmt);
    Status UpdateExecutor(UpdateStmt* stmt);
    Status DeleteExecutor(DeleteStmt* stmt);
    Status SelectExecutor(SelectStmt* stmt);
    Status DescribeTableExecutor(DescribeTableStmt* stmt);
    Status DropTableExecutor(DropTableStmt* stmt);
    Status TxnControlExecutor(TxnControlStmt* stmt);
    Status CreateModelExecutor(CreateModelStmt* stmt);
    Status DropModelExecutor(DropModelStmt* stmt);

    //expressions
    Status Eval(Expr* expr, Row* row, Datum* result);

    Status EvalLiteral(Literal* expr, Datum* result);
    Status EvalBinary(Binary* expr, Row* row, Datum* result);
    Status EvalUnary(Unary* expr, Row* row, Datum* result);
    Status EvalColRef(ColRef* expr, Datum* result);
    Status EvalColAssign(ColAssign* expr, Row* row, Datum* result);
    Status EvalCall(Call* expr, Row* row, Datum* result);
    Status EvalIsNull(IsNull* expr, Row* row, Datum* result);
    Status EvalScalarSubquery(ScalarSubquery* expr, Datum* result);
    Status EvalPredict(Predict* expr, Row* row, Datum* result);
    Status EvalCast(Cast* expr, Row* row, Datum* result);

    //TODO: make this Scan wrapper - this it the function SelectStmt should call rather than calling BeginScan/NextRow directly
//    Status Scan(WorkTable* scan, RowSet* result);
    
    Status BeginScan(Scan* scan);
    //TODO: these function names can be the same 'BeginScan' since the argument will overload it
    Status BeginScanConstant(ConstantTable* scan);
    Status BeginScanTable(PrimaryTable* scan);
    Status BeginScan(SelectScan* scan);
    Status BeginScan(ProductScan* scan);
    Status BeginScan(OuterSelectScan* scan);
    Status BeginScan(ProjectScan* scan);
    
    //TODO: these function names can be the same 'NextRow' since the argument will overload it
    Status NextRow(Scan* scan, Row** r);
    Status NextRowConstant(ConstantTable* scan, Row** r);
    Status NextRowTable(PrimaryTable* scan, Row** r);
    Status NextRow(SelectScan* scan, Row** r);
    Status NextRow(ProductScan* scan, Row** r);
    Status NextRow(OuterSelectScan* scan, Row** r);
    Status NextRow(ProjectScan* scan, Row** r);

    inline void ResetAggState() {
        is_agg_ = false;
        first_ = true;
        sum_ = Datum(0);
        count_ = Datum(0);
    }

private:
    Storage* storage_;
    Inference* inference_;
    Txn** txn_;
    std::vector<Row*> scopes_;


    //state for aggregate functions
    bool is_agg_;
    Datum min_;
    Datum max_;
    Datum sum_;
    Datum count_;
    bool first_;
};

}

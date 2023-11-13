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

    //expressions
    Status Eval(Expr* expr, Row* row, Datum* result);

    Status EvalLiteral(Literal* expr, Row* row, Datum* result);
    Status EvalBinary(Binary* expr, Row* row, Datum* result);
    Status EvalUnary(Unary* expr, Row* row, Datum* result);
    Status EvalColRef(ColRef* expr, Row* row, Datum* result);
    Status EvalColAssign(ColAssign* expr, Row* row, Datum* result);
    Status EvalCall(Call* expr, Row* row, Datum* result);
    Status EvalIsNull(IsNull* expr, Row* row, Datum* result);
    Status EvalScalarSubquery(ScalarSubquery* expr, Row* row, Datum* result);
    Status EvalPredict(Predict* expr, Row* row, Datum* result);

    //TODO: make this Scan wrapper - this it the function SelectStmt should call rather than calling BeginScan/NextRow directly
//    Status Scan(WorkTable* scan, RowSet* result);
    
    Status BeginScan(WorkTable* scan);
    Status BeginScanLeft(LeftJoin* scan);
    Status BeginScanFull(FullJoin* scan);
    Status BeginScanInner(InnerJoin* scan);
    Status BeginScanCross(CrossJoin* scan);
    Status BeginScanConstant(ConstantTable* scan);
    Status BeginScanTable(PrimaryTable* scan);
    
    Status NextRow(WorkTable* scan, Row** r);
    Status NextRowLeft(LeftJoin* scan, Row** r);
    Status NextRowFull(FullJoin* scan, Row** r);
    Status NextRowInner(InnerJoin* scan, Row** r);
    Status NextRowCross(CrossJoin* scan, Row** r);
    Status NextRowConstant(ConstantTable* scan, Row** r);
    Status NextRowTable(PrimaryTable* scan, Row** r);

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

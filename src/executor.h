#pragma once

#include "status.h"
#include "expr.h"
#include "stmt.h"
#include "storage.h"

namespace wsldb {


class Executor {
public:
    Executor(Storage* storage, Batch* batch);
    Status Execute(Stmt* stmt);
private:
    //statements
    Status CreateExecutor(CreateStmt* stmt);
    Status InsertExecutor(InsertStmt* stmt);
    Status UpdateExecutor(UpdateStmt* stmt);
    Status DeleteExecutor(DeleteStmt* stmt);
    Status SelectExecutor(SelectStmt* stmt);
    Status DescribeTableExecutor(DescribeTableStmt* stmt);
    Status DropTableExecutor(DropTableStmt* stmt);

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

private:
    Storage* storage_;
    Batch* batch_;
    QueryState qs_;
    std::vector<Row*> scopes_;
};

}

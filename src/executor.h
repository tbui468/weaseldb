#pragma once

#include "status.h"
#include "expr.h"
#include "stmt.h"
#include "storage.h"

namespace wsldb {


class Executor {
public:
    Executor(Storage* storage, Batch* batch);
    Status Verify(Stmt* stmt);
    Status Execute(Stmt* stmt);
private:
    //statements
    Status CreateVerifier(CreateStmt* stmt);
    Status CreateExecutor(CreateStmt* stmt);
    Status InsertVerifier(InsertStmt* stmt);
    Status InsertExecutor(InsertStmt* stmt);
    Status UpdateVerifier(UpdateStmt* stmt);
    Status UpdateExecutor(UpdateStmt* stmt);
    Status DeleteVerifier(DeleteStmt* stmt);
    Status DeleteExecutor(DeleteStmt* stmt);
    Status SelectVerifier(SelectStmt* stmt);
    Status SelectExecutor(SelectStmt* stmt);
    Status DescribeTableVerifier(DescribeTableStmt* stmt);
    Status DescribeTableExecutor(DescribeTableStmt* stmt);
    Status DropTableVerifier(DropTableStmt* stmt);
    Status DropTableExecutor(DropTableStmt* stmt);

    //expressions
    Status Verify(Expr* expr, DatumType* type);
    Status Eval(Expr* expr, Row* row, Datum* result);

    Status VerifyLiteral(Literal* expr, DatumType* type);
    Status VerifyBinary(Binary* expr, DatumType* type);
    Status VerifyUnary(Unary* expr, DatumType* type);
    Status VerifyColRef(ColRef* expr, DatumType* type);
    Status VerifyColAssign(ColAssign* expr, DatumType* type);
    Status VerifyCall(Call* expr, DatumType* type);
    Status VerifyIsNull(IsNull* expr, DatumType* type);
    Status VerifyScalarSubquery(ScalarSubquery* expr, DatumType* type);

    Status EvalLiteral(Literal* expr, Row* row, Datum* result);
    Status EvalBinary(Binary* expr, Row* row, Datum* result);
    Status EvalUnary(Unary* expr, Row* row, Datum* result);
    Status EvalColRef(ColRef* expr, Row* row, Datum* result);
    Status EvalColAssign(ColAssign* expr, Row* row, Datum* result);
    Status EvalCall(Call* expr, Row* row, Datum* result);
    Status EvalIsNull(IsNull* expr, Row* row, Datum* result);
    Status EvalScalarSubquery(ScalarSubquery* expr, Row* row, Datum* result);

    //TODO: rename WorkTable to scan
    Status Verify(WorkTable* scan, WorkingAttributeSet** working_attrs);
    Status VerifyLeft(LeftJoin* scan, WorkingAttributeSet** working_attrs); 
    Status VerifyFull(FullJoin* scan, WorkingAttributeSet** working_attrs);
    Status VerifyInner(InnerJoin* scan, WorkingAttributeSet** working_attrs);
    Status VerifyCross(CrossJoin* scan, WorkingAttributeSet** working_attrs);
    Status VerifyConstant(ConstantTable* scan, WorkingAttributeSet** working_attrs);
    Status VerifyTable(PrimaryTable* scan, WorkingAttributeSet** working_attrs);

    /*
     * Status Scan(WorkTable* scan, RowSet* result);
     * Status ScanLeft(LeftJoin* scan, RowSet* result);
     * Status ScanFull(FullJoin* scan, RowSet* result);
     * Status ScanInner(InnerJoin* scan, RowSet* result);
     * Status ScanCross(CrossJoin* scan, RowSet* result);
     * Status ScanConstant(ConstantTable* scan, RowSet* result);
     * Status ScanTable(PrimaryTable* scan, RowSet* result);
     */

    //Originally part of Stmt, but need it inside of Executor for Analyze/Execute functions to word
    Status OpenTable(QueryState& qs, const std::string& table_name, Table** table) {
        std::string serialized_table;
        TableHandle catalogue = qs.storage->GetTable(qs.storage->CatalogueTableName());
        bool ok = catalogue.db->Get(rocksdb::ReadOptions(), table_name, &serialized_table).ok();

        if (!ok)
            return Status(false, "Error: Table doesn't exist");

        *table = new Table(table_name, serialized_table);

        return Status(true, "ok");
    }
private:
    Storage* storage_;
    Batch* batch_;
    QueryState qs_;
    //proj_types only being filled in by SelectStmt
    //and only ScalarSubquery::Expr type checking it - probably doesn't need to exist...
    std::vector<std::vector<DatumType>> proj_types_;
};

}

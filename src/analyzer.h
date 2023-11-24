#pragma once

#include "inference.h"
#include "status.h"
#include "stmt.h"
#include "expr.h"
#include "storage.h"
#include "txn.h"

namespace wsldb {

class Analyzer {
public:
    Analyzer(Txn** txn): txn_(txn) {}
    Status Verify(Stmt* stmt, std::vector<DatumType>& types);
private:
    //statements
    Status CreateVerifier(CreateStmt* stmt);
    Status InsertVerifier(InsertStmt* stmt);
    Status UpdateVerifier(UpdateStmt* stmt);
    Status DeleteVerifier(DeleteStmt* stmt);
    Status SelectVerifier(SelectStmt* stmt, std::vector<DatumType>& types);
    Status DescribeTableVerifier(DescribeTableStmt* stmt);
    Status DropTableVerifier(DropTableStmt* stmt);
    Status TxnControlVerifier(TxnControlStmt* stmt);
    Status CreateModelVerifier(CreateModelStmt* stmt);
    Status DropModelVerifier(DropModelStmt* stmt);

    //expressions
    Status Verify(Expr* expr, DatumType* type);

    Status VerifyLiteral(Literal* expr, DatumType* type);
    Status VerifyBinary(Binary* expr, DatumType* type);
    Status VerifyUnary(Unary* expr, DatumType* type);
    Status VerifyColRef(ColRef* expr, DatumType* type);
    Status VerifyColAssign(ColAssign* expr, DatumType* type);
    Status VerifyCall(Call* expr, DatumType* type);
    Status VerifyIsNull(IsNull* expr, DatumType* type);
    Status VerifyScalarSubquery(ScalarSubquery* expr, DatumType* type);
    Status VerifyPredict(Predict* expr, DatumType* type);
    Status VerifyCast(Cast* expr, DatumType* type);

    Status Verify(Scan* scan, AttributeSet** working_attrs);

    Status VerifyConstant(ConstantScan* scan, AttributeSet** working_attrs);
    Status VerifyTable(TableScan* scan, AttributeSet** working_attrs);
    Status VerifySelectScan(SelectScan* scan, AttributeSet** working_attrs);
    Status Verify(ProductScan* scan, AttributeSet** working_attrs);
    Status Verify(OuterSelectScan* scan, AttributeSet** working_attrs);
    Status Verify(ProjectScan* scan, AttributeSet** working_attrs);

    Status GetSchema(const std::string& table_name, Table** schema) {
        *schema = nullptr;

        std::string serialized_schema;
        bool ok = (*txn_)->Get(Storage::Catalog(), table_name, &serialized_schema).Ok();

        if (!ok)
            return Status(false, "Analysis Error: Table with name '" + table_name + "' doesn't exist");

        *schema = new Table(table_name, serialized_schema);

        return Status();
    }

private:
    Txn** txn_;
    std::vector<AttributeSet*> scopes_;
};

}

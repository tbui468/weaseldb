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
    Status Verify(Stmt* stmt, AttributeSet** working_attrs);
private:
    //statements
    Status CreateVerifier(CreateStmt* stmt);
    Status InsertVerifier(InsertStmt* stmt);
    Status UpdateVerifier(UpdateStmt* stmt);
    Status DeleteVerifier(DeleteStmt* stmt);
    Status SelectVerifier(SelectStmt* stmt, AttributeSet** working_attrs);
    Status DescribeTableVerifier(DescribeTableStmt* stmt);
    Status DropTableVerifier(DropTableStmt* stmt);
    Status TxnControlVerifier(TxnControlStmt* stmt);
    Status CreateModelVerifier(CreateModelStmt* stmt);
    Status DropModelVerifier(DropModelStmt* stmt);

    //expressions
    Status Verify(Expr* expr, Attribute* attr);

    Status VerifyLiteral(Literal* expr, Attribute* attr);
    Status VerifyBinary(Binary* expr, Attribute* attr);
    Status VerifyUnary(Unary* expr, Attribute* attr);
    Status VerifyColRef(ColRef* expr, Attribute* attr);
    Status VerifyColAssign(ColAssign* expr, Attribute* attr);
    Status VerifyCall(Call* expr, Attribute* attr);
    Status VerifyIsNull(IsNull* expr, Attribute* attr);
    Status VerifyScalarSubquery(ScalarSubquery* expr, Attribute* attr);
    Status VerifyPredict(Predict* expr, Attribute* attr);
    Status VerifyCast(Cast* expr, Attribute* attr);

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
    bool has_agg_ {false};
};

}

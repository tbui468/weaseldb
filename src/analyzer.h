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

    //TODO: rename WorkTable to scan
    Status Verify(WorkTable* scan, AttributeSet** working_attrs);

    Status VerifyLeft(LeftJoin* scan, AttributeSet** working_attrs); 
    Status VerifyFull(FullJoin* scan, AttributeSet** working_attrs);
    Status VerifyInner(InnerJoin* scan, AttributeSet** working_attrs);
    Status VerifyCross(CrossJoin* scan, AttributeSet** working_attrs);
    Status VerifyConstant(ConstantTable* scan, AttributeSet** working_attrs);
    Status VerifyTable(PrimaryTable* scan, AttributeSet** working_attrs);

    Status GetSchema(const std::string& table_name, Schema** schema) {
        *schema = nullptr;

        std::string serialized_schema;
        bool ok = (*txn_)->Get(Storage::Catalog(), table_name, &serialized_schema).Ok();

        if (!ok)
            return Status(false, "Analysis Error: Table with name '" + table_name + "' doesn't exist");

        *schema = new Schema(table_name, serialized_schema);

        return Status();
    }

    Status GetAttribute(Attribute* attr, int* idx, int* scope, const std::string& table_ref_param, const std::string& col) const {
        for (int i = scopes_.size() - 1; i >= 0; i--) {
            Status s = GetAttributeAtScopeOffset(attr, idx, scope, table_ref_param, col, i);
            if (s.Ok() || i == 0)
                return s;
        }

        return Status(false, "should never reach this"); //keeping compiler quiet
    }
    Status GetAttributeAtScopeOffset(Attribute* attr, int* idx, int* scope, const std::string& table_ref_param, const std::string& col, int i) const {
        std::string table_ref = table_ref_param;
        AttributeSet* as = scopes_.at(i);
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

        *attr = as->GetAttribute(table_ref, col);
        *idx = as->GetAttributeIdx(table_ref, col);
        *scope = scopes_.size() - 1 - i;
        
        return Status();
    }
private:
    Txn** txn_;
    std::vector<AttributeSet*> scopes_;
};

}

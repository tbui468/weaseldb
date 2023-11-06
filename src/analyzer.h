#pragma once

#include "storage.h"
#include "status.h"
#include "stmt.h"
#include "expr.h"

namespace wsldb {

class Analyzer {
public:
    Analyzer(Storage* storage);
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

    //TODO: rename WorkTable to scan
    Status Verify(WorkTable* scan, WorkingAttributeSet** working_attrs);

    Status VerifyLeft(LeftJoin* scan, WorkingAttributeSet** working_attrs); 
    Status VerifyFull(FullJoin* scan, WorkingAttributeSet** working_attrs);
    Status VerifyInner(InnerJoin* scan, WorkingAttributeSet** working_attrs);
    Status VerifyCross(CrossJoin* scan, WorkingAttributeSet** working_attrs);
    Status VerifyConstant(ConstantTable* scan, WorkingAttributeSet** working_attrs);
    Status VerifyTable(PrimaryTable* scan, WorkingAttributeSet** working_attrs);

    //TODO: similar function in Executor - should merge them
    Status OpenTable(const std::string& table_name, Table** table) {
        std::string serialized_table;
        TableHandle catalogue = storage_->GetTable(storage_->CatalogueTableName());
        bool ok = catalogue.db->Get(rocksdb::ReadOptions(), table_name, &serialized_table).ok();

        if (!ok)
            return Status(false, "Error: Table doesn't exist");

        *table = new Table(table_name, serialized_table);

        return Status();
    }
    Status GetWorkingAttribute(WorkingAttribute* attr, const std::string& table_ref_param, const std::string& col) const {
        for (int i = scopes_.size() - 1; i >= 0; i--) {
            Status s = GetWorkingAttributeAtScopeOffset(attr, table_ref_param, col, i);
            if (s.Ok() || i == 0)
                return s;
        }

        return Status(false, "should never reach this"); //keeping compiler quiet
    }
    Status GetWorkingAttributeAtScopeOffset(WorkingAttribute* attr, const std::string& table_ref_param, const std::string& col, int i) const {
        std::string table_ref = table_ref_param;
        WorkingAttributeSet* as = scopes_.at(i);
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
        attr->scope = scopes_.size() - 1 - i;
        
        return Status();
    }
private:
    Storage* storage_;
    std::vector<WorkingAttributeSet*> scopes_;
};

}

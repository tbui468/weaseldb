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
    //TODO: need Verify and Eval for Expr* too
private:
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

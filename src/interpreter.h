#pragma once

#include <string>

#include "stmt.h"
#include "row.h"
#include "status.h"

namespace wsldb {

class DB;

class Interpreter {
public:
    Interpreter(const std::string& cluster_path): cluster_path_(cluster_path) {}
    Status ExecuteStmts(std::vector<Stmt*> stmts, DB* db) { //TODO: argument should be Transaction pointer in the future
        for (Stmt* stmt: stmts) {
            //query state is destroyed when each query is processed
            QueryState qs(db);
            std::vector<TokenType> types;
            Status status = stmt->Analyze(qs, types);

            if (status.Ok()) {
                status = stmt->Execute(qs);
            }
           
            if (status.Ok()) { 
                RowSet* tupleset = status.Tuples();
                if (tupleset) {
                    tupleset->Print();
                }
            }

            if (!status.Ok()) {
                std::cout << status.Msg() << std::endl;
                return status; //for now just ending stmt execution if any goes wrong
            }
        }

        return Status(true, "ok");
    }
private:
    std::string cluster_path_;
    //TODO: should store query state here, and pass in :ls

    //Should pass this to Stmt::Execute(QueryState* qs);
    //state for aggregate functions
    Datum min_;
    Datum max_;
    Datum sum_;
    Datum count_;
    bool first_;
    std::vector<WorkingAttributeSet*> analysis_scopes_;
    std::vector<Row*> scope_rows_;
};

}

#pragma once

#include <string>
#include <unordered_map>

#include "stmt.h"
#include "row.h"
#include "status.h"
#include "storage.h"
#include "rocksdb/db.h"

namespace wsldb {

class Interpreter {
public:
    Interpreter(Storage* storage): storage_(storage) {}

    Status ExecuteTxn(Txn& txn, std::unordered_map<std::string, rocksdb::WriteBatch*>& batches) {
        //Issue: we still need to have the system catalog for the analyze stage
        //So completely avoiding Storage module here is impossible with current architecture
        //would restructuring help here?  Interpreter stores Storage* pointer, and uses that during analysis
        //stage.  This makes a lot more sense.  It's easier to reason about. Interpreter can then execute
        //and analyze while maintaining all info necessary as state.  

        return ExecuteStmts(txn.stmts);
    }

    Status ExecuteStmts(std::vector<Stmt*> stmts) { //TODO: argument should be Transaction pointer in the future
        for (Stmt* stmt: stmts) {
            //query state is destroyed when each query is processed
            QueryState qs(storage_);
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
    Storage* storage_;
    //TODO: should store query state here, and pass in :ls

    //TODO: should rewrite interpreter so that it calls Executors for each statement type
    //This allows us to store query state directly in interpreter (which makes a lot more sense)
    //and we can get rid of QueryState class completely
    Datum min_;
    Datum max_;
    Datum sum_;
    Datum count_;
    bool first_;
    std::vector<WorkingAttributeSet*> analysis_scopes_;
    std::vector<Row*> scope_rows_;
};

}

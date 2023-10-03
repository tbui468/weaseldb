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

    Status ExecuteTxn(Txn& txn) {
        Batch batch;
        Status s;
        for (Stmt* stmt: txn.stmts) {
            QueryState qs(storage_, &batch);
            std::vector<TokenType> types;
            s = stmt->Analyze(qs, types);

            if (s.Ok()) {
                s = stmt->Execute(qs);
            }
           
            if (s.Ok()) { 
                RowSet* tupleset = s.Tuples();
                if (tupleset) {
                    tupleset->Print();
                }
            } else {
                std::cout << s.Msg() << std::endl;
                break;
            }
        }

        //TODO: NOT atomic when writing across mulitple tables
        //Need to implement that separately (possibly with using transactions API in rocksdb)
        if (s.Ok() && txn.commit_on_success)
            batch.Write(*storage_);

        //TODO: what status do we return if some transactions fail, but some succeed?
        //Could return a vector of Status objects, one for each transaction
        return s;
    }
private:
    Storage* storage_;
};

}
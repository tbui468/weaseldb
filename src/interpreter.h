#pragma once

#include <string>
#include <unordered_map>

#include "stmt.h"
#include "row.h"
#include "status.h"
#include "storage.h"
#include "rocksdb/db.h"
#include "executor.h"

namespace wsldb {

class Interpreter {
public:
    Interpreter(Storage* storage): storage_(storage) {}

    Status ExecuteTxn(Txn& txn) {
        Batch batch;
        Status s;
        for (Stmt* stmt: txn.stmts) {
            Executor e(storage_, &batch);
            std::vector<DatumType> types;
            s = e.Verify(stmt, types);
            if (!s.Ok())
                break;

            s = e.Execute(stmt);
            if (!s.Ok())
                break;
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

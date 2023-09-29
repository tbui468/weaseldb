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
        std::unordered_map<std::string, rocksdb::WriteBatch*> batches;

        Status s = ExecuteStmts(txn.stmts);

        if (!s.Ok())
           return s; 

        for (const std::pair<const std::string, rocksdb::WriteBatch*>& p: batches) {
            rocksdb::DB* handle = storage_->GetIdxHandle(p.first);
            handle->Write(rocksdb::WriteOptions(), p.second);
        }

        return s;
    }

    Status ExecuteStmts(std::vector<Stmt*> stmts) { //TODO: argument should be Transaction pointer in the future
        for (Stmt* stmt: stmts) {
            Batch batch;
            QueryState qs(storage_, &batch);
            std::vector<TokenType> types;
            Status status = stmt->Analyze(qs, types);

            if (status.Ok()) {
                status = stmt->Execute(qs);
            }
           
            if (status.Ok()) { 
                //TODO: NOT atomic when writing across mulitple tables
                //Need to implement that separately (possibly with using transactions API in rocksdb)
                batch.Write(*storage_);
                RowSet* tupleset = status.Tuples();
                if (tupleset) {
                    tupleset->Print();
                }
            } else {
                std::cout << status.Msg() << std::endl;
                continue;
            }
        }

        //TODO: what status do we return if some transactions fail, but some succeed?
        //Could return a vector of Status objects, one for each transaction
        return Status(true, "ok");
    }
private:
    Storage* storage_;
};

}

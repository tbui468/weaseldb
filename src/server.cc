#include <unordered_map>

#include "server.h"
#include "tokenizer.h"
#include "parser.h"
#include "interpreter.h"
#include "rocksdb/db.h"

namespace wsldb {

Status Server::RunQuery(const std::string& query, Storage* storage) {
    Tokenizer tokenizer(query);
    std::vector<Token> tokens = std::vector<Token>();

    do {
        tokens.push_back(tokenizer.NextToken());
    } while (tokens.back().type != TokenType::Eof);

    Parser parser(tokens);
    std::vector<Stmt*> stmts = parser.ParseStmts();
    std::vector<Txn> txns = Parser::GroupIntoTxns(stmts);

    Interpreter interp(storage);

    for (Txn txn: txns) {
        std::unordered_map<std::string, rocksdb::WriteBatch*> batches;
        Status s = interp.ExecuteTxn(txn, batches);

        //TODO: currently not writing data to batches in Stmt::Execute - need to do this for transactions to work
        /*
        for (const std::pair<const std::string, rocksdb::WriteBatch*>& p: batches) {
            rocksdb::DB* handle = storage->GetIdxHandle(p.first);
            handle->Write(rocksdb::WriteOptions(), p.second);
        }*/
        
        if (!s.Ok())
            return s;
    }

    return Status(true, "ok");
}

}

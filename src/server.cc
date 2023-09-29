#include <unordered_map>

#include "server.h"
#include "tokenizer.h"
#include "parser.h"
#include "interpreter.h"
#include "rocksdb/db.h"

namespace wsldb {

//TODO: Sever shouldn't be doing this - should spawn a backend process to handle query
Status Server::RunQuery(const std::string& query, Storage* storage) {
    Tokenizer tokenizer(query);
    std::vector<Token> tokens = std::vector<Token>();

    do {
        tokens.push_back(tokenizer.NextToken());
    } while (tokens.back().type != TokenType::Eof);

    Parser parser(tokens);
    std::vector<Txn> txns = parser.ParseTxns();

    Interpreter interp(storage);

    for (Txn txn: txns) {
        Status s = interp.ExecuteTxn(txn);
    }

    return Status(true, "ok");
}

}

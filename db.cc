#include "db.h"
#include "tokenizer.h"
#include "parser.h"

namespace wsldb {

DB::DB(const std::string& path) {
    rocksdb::Options options_;
    options_.create_if_missing = true;
    status_ = rocksdb::DB::Open(options_, path, &db_);
    if (!status_.ok()) {
        std::cout << "Error:" << status_.ToString() << std::endl;
    }
}

DB::~DB() {
    delete db_;
}

std::vector<Token> DB::tokenize(const std::string& query) {
    Tokenizer tokenizer(query);
    std::vector<Token> tokens = std::vector<Token>();

    do {
        tokens.push_back(tokenizer.NextToken());
    } while (tokens.back().type != TokenType::Eof);

    //tokenizer.DebugPrintTokens(tokens);

    return tokens;
}

std::vector<Stmt*> DB::parse(const std::vector<Token>& tokens) {
    Parser parser(tokens);
    return parser.ParseStmts();
}

rocksdb::Status DB::execute(const std::string& query) {
    std::vector<Token> tokens = tokenize(query);
    std::vector<Stmt*> ptree = parse(tokens);
    for (Stmt* s: ptree) {
        s->DebugPrint();
    }
    /*
    std::vector<QueryTree> qtree = analyze(ptree);
    std::vector<RewrittenTree> rwtree = rewrite(qtree);
    std::vector<PlanTree> plantree = plan(rwtree);
    std::string msg = execute_plan(plantree);*/

    //tokenize query
    //parse into query tree
    //execute query tree starting from the leaves
    return status_;
}

}

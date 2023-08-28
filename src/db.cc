#include "db.h"
#include "tokenizer.h"
#include "parser.h"

namespace wsldb {

DB::DB(const std::string& path): path_(path) {
    rocksdb::Options options_;
    options_.create_if_missing = true;
    status_ = rocksdb::DB::Open(options_, path, &catalogue_handle_);
    if (!status_.ok()) {
        std::cout << "Error:" << status_.ToString() << std::endl;
    }

    //TODO: iterate through catalog keys/values and open all database handles
}

DB::~DB() {
    delete catalogue_handle_;
}

std::vector<Token> DB::tokenize(const std::string& query) {
    Tokenizer tokenizer(query);
    std::vector<Token> tokens = std::vector<Token>();

    do {
        tokens.push_back(tokenizer.NextToken());
    } while (tokens.back().type != TokenType::Eof);

//    tokenizer.DebugPrintTokens(tokens);

    return tokens;
}

std::vector<Stmt*> DB::parse(const std::vector<Token>& tokens) {
    Parser parser(tokens);
    return parser.ParseStmts();
}

rocksdb::Status DB::execute(const std::string& query) {
    std::vector<Token> tokens = tokenize(query);
    std::vector<Stmt*> ptree = parse(tokens);
    /*
    for (Stmt* s: ptree) {
        s->DebugPrint();
    }*/

    for (Stmt* s: ptree) {
        s->Analyze(this);
        s->Execute(this);
    }

    /*
    std::vector<RewrittenTree> rwtree = rewrite(qtree);
    std::vector<PlanTree> plantree = plan(rwtree);
    std::string msg = execute_plan(plantree);*/

    //tokenize query
    //parse into query tree
    //execute query tree starting from the leaves
    return status_;
}

void DB::ShowTables() {
    rocksdb::Iterator* it = catalogue_handle_->NewIterator(rocksdb::ReadOptions());

    std::cout << "Tables:" << std::endl;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();

        //deserialize schema and print out all values
        int off = 0;
        int count = *((int*)(value.data() + off));
        off += sizeof(int);

        std::cout << key << std::endl;

        for (int i = 0; i < count; i++) {
            //read data type
            TokenType attr_type = *((TokenType*)(value.data() + off));
            off += sizeof(TokenType);
            int attr_name_size = *((int*)(value.data() + off));
            off += sizeof(int);
            std::string attr_name = value.substr(off, attr_name_size);
            off += attr_name_size;
            std::cout << attr_name << ": " << TokenTypeToString(attr_type) << std::endl;
        }

        //looop through attribute types and names, and print out
    }

    delete it;
}

void DB::AppendRelationHandle(rocksdb::DB* h) {
    relation_handles_.push_back(h);    
}

}

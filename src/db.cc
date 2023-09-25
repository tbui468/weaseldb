#include <fstream>

#include "db.h"
#include "tokenizer.h"
#include "parser.h"
#include "interpreter.h"

namespace wsldb {

DB::DB(const std::string& path): path_(path) {
    rocksdb::Options options_;
    options_.create_if_missing = true;
    status_ = rocksdb::DB::Open(options_, path, &catalogue_handle_);
    if (!status_.ok()) {
        std::cout << "Error:" << status_.ToString() << std::endl;
    }
}

DB::~DB() {
    for (rocksdb::DB* h: idx_handles_) {
        delete h;
    }
    delete catalogue_handle_;
}


std::string DB::PrependDBPath(const std::string& idx_name) {
    return path_ + "/" + idx_name;
}

std::vector<Token> DB::tokenize(const std::string& query) {
    Tokenizer tokenizer(query);
    std::vector<Token> tokens = std::vector<Token>();

    do {
        tokens.push_back(tokenizer.NextToken());
    } while (tokens.back().type != TokenType::Eof);


    return tokens;
}

std::vector<Stmt*> DB::parse(const std::vector<Token>& tokens) {
    Parser parser(tokens);
    return parser.ParseStmts(this);
}

wsldb::Status DB::ExecuteScript(const std::string& path) {
    std::string line;
    std::ifstream script(path);
    std::vector<Token> tokens;
    while (getline(script, line)) {
        std::vector<Token> temp = tokenize(line);
        if (!tokens.empty()) {
            tokens.pop_back(); //remove Eof token at end of each line
        }
        tokens.insert(tokens.end(), std::make_move_iterator(temp.begin()), std::make_move_iterator(temp.end()));
    }
    script.close();


    //Tokenizer::DebugPrintTokens(tokens);

    std::vector<Stmt*> ptree = parse(tokens);

    Interpreter interp(path_);
    Status s = interp.ExecuteStmts(ptree, this);

    /*
    for (Stmt* s: ptree) {
        std::vector<TokenType> types;
        Status status = s->Analyze(types);

        if (status.Ok()) 
            status = s->Execute();
       
        if (status.Ok()) { 
            RowSet* tupleset = status.Tuples();
            if (tupleset) {
                tupleset->Print();
            }
        }

        if (!status.Ok())
            std::cout << status.Msg() << std::endl;
    }*/

    return s;
}

void DB::CreateIdxHandle(const std::string& idx_name) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* idx_handle;
    rocksdb::Status status = rocksdb::DB::Open(options, PrependDBPath(idx_name), &idx_handle);
    idx_handles_.push_back(idx_handle);    
}

rocksdb::DB* DB::GetIdxHandle(const std::string& idx_name) {
    for (rocksdb::DB* h: idx_handles_) {
        if (h->GetName().compare(PrependDBPath(idx_name)) == 0)
            return h;
    }

    //open idx if not found, and put into idx_handles_ vector
    rocksdb::Options options;
    options.create_if_missing = false;
    rocksdb::DB* idx_handle;
    rocksdb::Status status = rocksdb::DB::Open(options, PrependDBPath(idx_name), &idx_handle);
    idx_handles_.push_back(idx_handle);    

    return idx_handle;
}

void DB::DropIdxHandle(const std::string& idx_name) {
    for (size_t i = 0; i < idx_handles_.size(); i++) {
        rocksdb::DB* h = idx_handles_.at(i);
        if (h->GetName().compare(PrependDBPath(idx_name)) == 0) {
            delete h;
            idx_handles_.erase(idx_handles_.begin() + i);
            break;
        }
    }

    rocksdb::Options options;
    rocksdb::DestroyDB(PrependDBPath(idx_name), options);
}


}

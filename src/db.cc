#include <fstream>

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
    rocksdb::Iterator* it = catalogue_handle_->NewIterator(rocksdb::ReadOptions());

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string table_name = it->key().ToString();
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* rel_handle;
        rocksdb::Status status = rocksdb::DB::Open(options, GetTablePath(table_name), &rel_handle);
        AppendTableHandle(rel_handle);
    }
}

DB::~DB() {
    for (rocksdb::DB* h: table_handles_) {
        delete h;
    }
    delete catalogue_handle_;
}


std::string DB::GetTablePath(const std::string& table_name) {
    return GetPath() + "/" + table_name;
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

//TODO: need to reimplement this since we changed alot during the writing of ExecuteScript
rocksdb::Status DB::execute(const std::string& query) {
    std::vector<Token> tokens = tokenize(query);
    std::vector<Stmt*> ptree = parse(tokens);
    /*
    for (Stmt* s: ptree) {
        std::cout << s->ToString() << std::endl;
    }*/

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

        std::cout << status.Msg() << std::endl;
    }

    return status_;
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

        //disabling status messages for tests
        //std::cout << status.Msg() << std::endl;
    }

    return Status(true, "ok");
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

void DB::AppendTableHandle(rocksdb::DB* h) {
    table_handles_.push_back(h);    
}

rocksdb::DB* DB::GetTableHandle(const std::string& table_name) {
    for (rocksdb::DB* h: table_handles_) {
        if (h->GetName().compare(GetTablePath(table_name)) == 0)
            return h;
    }
    return NULL;
}

bool DB::TableSchema(const std::string& table_name, std::string* serialized_schema) {
    rocksdb::Status status = Catalogue()->Get(rocksdb::ReadOptions(), table_name, serialized_schema);
    return status.ok();
}

bool DB::DropTable(const std::string& table_name) {
    for (int i = 0; i < table_handles_.size(); i++) {
        rocksdb::DB* h = table_handles_.at(i);
        if (h->GetName().compare(GetTablePath(table_name)) == 0) {
            delete h;
            rocksdb::Options options;
            rocksdb::DestroyDB(GetTablePath(table_name), options);
            table_handles_.erase(table_handles_.begin() + i);
            return true;
        }
    }

    return false;
}

}

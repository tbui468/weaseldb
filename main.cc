#include <cassert>
#include <iostream>
#include <vector>
#include "rocksdb/db.h"

enum class TokenType {
    /* SQL keywords */
    Create,
    Table, 
    Text,
    Int,
    Primary,
    Key,
    Insert,
    Into,
    Values,

    /* user-defined identifier */
    Identifier,

    /* literals */
    IntLiteral,
    StringLiteral,

    /* single characters */
    LParen,
    RParen,
    Comma,
    SemiColon,
    Eof,

    /* error */
    Error
};

class Token {
public:
    Token(std::string lexeme, TokenType type): lexeme(std::move(lexeme)), type(type) {}
    std::string lexeme;
    TokenType type;
};

/*
enum class ValueType {
    String,
    Integer
    //Null
};

class Value {
public:
    Value(Token t) {
        switch (t.type) {
            case TokenType::IntLiteral:
                type_ = ValueType::Integer;
                //TODO:
                break;
            case TokenType::StringLiteral:
                type_ = ValueType::String;
                //TODO:
                break;
            default:
                std::cout << "token type not supported as possible value" << std::endl;
                break;
        }
    }

    std::string AsString() {
        return as_.string;
    }

    int AsInteger() {
        return as_.integer;
    }

private:
    ValueType type_;
    union {
        int integer;
        std::string string;
    } as_;
};*/

class Expr {
public:
    virtual void Eval() = 0;
    virtual std::string ToString() = 0;
};

class IntLiteral: public Expr {
public:
    IntLiteral(Token t): t_(t) {}
    void Eval() override {
    }
    virtual std::string ToString() {
        return t_.lexeme;
    }
private:
    Token t_;
};

class StringLiteral: public Expr {
public:
    StringLiteral(Token t): t_(t) {}
    void Eval() override {
    }
    virtual std::string ToString() {
        return t_.lexeme;
    }
private:
    Token t_;
};

class Tuple {
public:
    Tuple(std::vector<Expr*> exprs): exprs(std::move(exprs)) {}
    std::vector<Expr*> exprs;
};

class Stmt {
public:
    virtual void Execute() = 0;
    virtual void DebugPrint() = 0;
};

class CreateStmt: public Stmt {
public:
    CreateStmt(Token target, std::vector<Token> attrs, std::vector<Token> types):
        target_(target), attrs_(std::move(attrs)), types_(std::move(types)) {}

    void Execute() override {
        //
    }
    void DebugPrint() override {
        std::cout << "Create:\n";
        std::cout << "\ttable name: " << target_.lexeme << std::endl;
        int i = 0;
        for (const Token& t: attrs_) {
            std::cout << "\t" << t.lexeme << ": " << types_.at(i).lexeme << std::endl;
            i++;
        }
    }
private:
    Token target_;
    std::vector<Token> attrs_;
    std::vector<Token> types_;
};

class InsertStmt: public Stmt {
public:
    InsertStmt(Token target, std::vector<Tuple*> values):
        target_(target), values_(std::move(values)) {}

    void Execute() override {
        //
    }
    void DebugPrint() override {
        std::cout << "Insert:\n";
        std::cout << "\ttarget: " << target_.lexeme << std::endl;
        for (Tuple* tuple: values_) {
            std::cout << "\t";
            for (Expr* e: tuple->exprs) {
                std::cout << e->ToString() << ", ";
            }
            std::cout << "\n";
        }
    }
private:
    Token target_;
    std::vector<Tuple*> values_; //Tuples are lists of expressions
};

class SelectStmt: public Stmt {
public:
    //must implement execute
private:
};

class Parser {
public:
    Parser(std::vector<Token> tokens): tokens_(std::move(tokens)), idx_(0) {}

    std::vector<Stmt*> ParseStmts() {
        std::vector<Stmt*> stmts = std::vector<Stmt*>();

        while (PeekToken().type != TokenType::Eof) {
            stmts.push_back(ParseStmt());    
        }
        
        return stmts;
    }
private:
    Expr* ParsePrimaryExpr() {
        switch (PeekToken().type) {
            case TokenType::IntLiteral:
                return new IntLiteral(NextToken());
            case TokenType::StringLiteral:
                return new StringLiteral(NextToken());
            default:
                return NULL;
        }
        return NULL;
    }
    Expr* ParseExpr() {
        return ParsePrimaryExpr();
    }

    Tuple* ParseTuple() {
        NextToken(); //(
        std::vector<Expr*> exprs = std::vector<Expr*>();
        while (PeekToken().type != TokenType::RParen) {
            exprs.push_back(ParseExpr());
            if (PeekToken().type == TokenType::Comma) {
                NextToken(); //,
            }
        }
        NextToken(); //)
        return new Tuple(exprs);
    }

    Token PeekToken() {
        return tokens_.at(idx_);
    }
    Token NextToken() {
        return tokens_.at(idx_++);
    }

    Stmt* ParseStmt() {
        switch (NextToken().type) {
            case TokenType::Create: {
                NextToken(); //table
                Token target = NextToken();
                NextToken(); //(
                std::vector<Token> attrs;
                std::vector<Token> types;
                while (PeekToken().type != TokenType::RParen) {
                    attrs.push_back(NextToken());
                    types.push_back(NextToken());
                    if (PeekToken().type == TokenType::Primary) {
                        NextToken(); //primary
                        NextToken(); //key
                    }

                    if (PeekToken().type == TokenType::Comma) {
                        NextToken();//,
                    }
                }

                NextToken();//)
                NextToken();//;
                return new CreateStmt(target, attrs, types);
            }
            case TokenType::Insert: {
                NextToken(); //into
                Token target = NextToken(); //table name
                NextToken(); //values
                std::vector<Tuple*> values = std::vector<Tuple*>();
                while (PeekToken().type == TokenType::LParen) {
                    values.push_back(ParseTuple());
                    if (PeekToken().type == TokenType::Comma) {
                        NextToken(); //,
                    }
                }
                NextToken(); //;
                return new InsertStmt(target, values);
            }
        }

        return NULL;
    }
private:
    int idx_;
    std::vector<Token> tokens_;
};

class Tokenizer {
public:
    Tokenizer(const std::string& query): query_(query), idx_(0) {}

    Token NextToken() {
        if (AtEnd()) return MakeToken(TokenType::Eof);
        SkipWhitespace();
        if (AtEnd()) return MakeToken(TokenType::Eof);

        if (IsAlpha(query_.at(idx_))) return MakeIdentifier();
        if (IsNumeric(query_.at(idx_))) return MakeNumber();
        if (query_.at(idx_) == '\'') return MakeString();

        switch (query_.at(idx_)) {
            case '(':
                return MakeToken(TokenType::LParen);
            case ')':
                return MakeToken(TokenType::RParen);
            case ',':
                return MakeToken(TokenType::Comma);
            case ';':
                return MakeToken(TokenType::SemiColon);
            default:
                return MakeToken(TokenType::Error);
        }

    }

    void DebugPrintTokens(const std::vector<Token>& tokens) {
        for (const Token& t: tokens) {
            switch (t.type) {
                case TokenType::Create:
                    std::cout << "Create" <<std::endl;
                    break;
                case TokenType::Table:
                    std::cout << "Table" <<std::endl;
                    break;
                case TokenType::Text:
                    std::cout << "Text" <<std::endl;
                    break;
                case TokenType::Int:
                    std::cout << "Int" <<std::endl;
                    break;
                case TokenType::Primary:
                    std::cout << "Primary" <<std::endl;
                    break;
                case TokenType::Key:
                    std::cout << "Key" <<std::endl;
                    break;
                case TokenType::Insert:
                    std::cout << "Insert" <<std::endl;
                    break;
                case TokenType::Into:
                    std::cout << "Into" <<std::endl;
                    break;
                case TokenType::Values:
                    std::cout << "Values" <<std::endl;
                    break;
                case TokenType::Identifier:
                    std::cout << "Identifier: " << t.lexeme << std::endl;
                    break;
                case TokenType::Eof:
                    std::cout << "Eof" <<std::endl;
                    break;
                case TokenType::IntLiteral:
                    std::cout << "IntLiteral: " << t.lexeme << std::endl;
                    break;
                case TokenType::StringLiteral:
                    std::cout << "StringLiteral: " << t.lexeme << std::endl;
                    break;
                case TokenType::LParen:
                    std::cout << "LParen" <<std::endl;
                    break;
                case TokenType::RParen:
                    std::cout << "RParen" <<std::endl;
                    break;
                case TokenType::Comma:
                    std::cout << "Comma" <<std::endl;
                    break;
                case TokenType::SemiColon:
                    std::cout << "SemiColon" <<std::endl;
                    break;
                case TokenType::Error:
                    std::cout << "Error" << std::endl;
                    break;
                default:
                    std::cout << "Invalid token" << std::endl;
                    break;
            }
        }
    }
private:
    Token MakeToken(TokenType type) {
        return Token(query_.substr(idx_++, 1), type);
    }

    Token MakeIdentifier() {
        int idx = idx_;
        int len = 0;

        while (IsNumeric(query_.at(idx_)) || IsAlpha(query_.at(idx_))) {
            idx_++;
            len++;
        }

        TokenType type;

        if (len == 6 && strncmp(&query_.at(idx), "create", len) == 0) {
            type = TokenType::Create; 
        } else if (len == 5 && strncmp(&query_.at(idx), "table", len) == 0) {
            type = TokenType::Table;
        } else if (len == 4 && strncmp(&query_.at(idx), "text", len) == 0) {
            type = TokenType::Text;
        } else if (len == 3 && strncmp(&query_.at(idx), "int", len) == 0) {
            type = TokenType::Int;
        } else if (len == 7 && strncmp(&query_.at(idx), "primary", len) == 0) {
            type = TokenType::Primary;
        } else if (len == 3 && strncmp(&query_.at(idx), "key", len) == 0) {
            type = TokenType::Key;
        } else if (len == 6 && strncmp(&query_.at(idx), "insert", len) == 0) {
            type = TokenType::Insert;
        } else if (len == 4 && strncmp(&query_.at(idx), "into", len) == 0) {
            type = TokenType::Into;
        } else if (len == 6 && strncmp(&query_.at(idx), "values", len) == 0) {
            type = TokenType::Values;
        } else {
            type = TokenType::Identifier;
        }

        return Token(query_.substr(idx, len), type);
    }

    Token MakeString() {
        idx_++; //skip starting single quote
        int idx = idx_;
        int len = 0;
        TokenType type = TokenType::StringLiteral;
        while (query_.at(idx_) != '\'') {
            idx_++;
            len++;
        }

        idx_++; //skip last single quote
        return Token(query_.substr(idx, len), type);
    }

    Token MakeNumber() {
        int idx = idx_;
        int len = 0;
        TokenType type = TokenType::IntLiteral;
        while (IsNumeric(query_.at(idx_))) {
            idx_++;
            len++;
        }
        return Token(query_.substr(idx, len), type);
    }

    void SkipWhitespace() {
        while (query_.at(idx_) == ' ' || 
               query_.at(idx_) == '\n' || 
               query_.at(idx_) == '\t') {
            idx_++;
        }
    }

    bool IsAlpha(char c) {
        return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_';
    }

    bool IsNumeric(char c) {
        return ('0' <= c && c <= '9') || c == '.';
    }

    bool AtEnd() {
        return idx_ >= query_.length();
    }
private:
    int idx_;
    std::string query_;
};

class WeaselDB {
public:
    WeaselDB(const std::string& path) {
        rocksdb::Options options_;
        options_.create_if_missing = true;
        status_ = rocksdb::DB::Open(options_, path, &db_);
        if (!status_.ok()) {
            std::cout << "Error:" << status_.ToString() << std::endl;
        }
    }

    virtual ~WeaselDB() {
        delete db_;
    }

    std::vector<Token> tokenize(const std::string& query) {
        Tokenizer tokenizer(query);
        std::vector<Token> tokens = std::vector<Token>();

        do {
            tokens.push_back(tokenizer.NextToken());
        } while (tokens.back().type != TokenType::Eof);

        //tokenizer.DebugPrintTokens(tokens);

        return tokens;
    }

    std::vector<Stmt*> parse(const std::vector<Token>& tokens) {
        Parser parser(tokens);
        return parser.ParseStmts();
    }

    rocksdb::Status execute(const std::string& query) {
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

    /*
    rocksdb::Status InsertRecord(int key, int v1, const std::string& v2) {
        std::string key_str((char*)&key, sizeof(key));
        std::string v1_str((char*)&v1, sizeof(int));
        status_ = db_->Put(rocksdb::WriteOptions(), key_str, v1_str + v2);
        return status_;
    }

    rocksdb::Status GetRecord(int key, WeaselRecord* rec) {
        std::string key_str((char*)&key, sizeof(key));
        std::string result;
        status_ = db_->Get(rocksdb::ReadOptions(), key_str, &result);
        if (!status_.ok()) {
            return status_;
        }
        rec->v1 = *((int*)(result.data()));
        rec->v2 = result.substr(sizeof(int), result.length() - sizeof(int));
        return status_;
    }

    rocksdb::Status DeleteRecord(int key) {
        std::string key_str((char*)&key, sizeof(key));
        status_ = db_->Delete(rocksdb::WriteOptions(), key_str);
        return status_;
    }

    std::vector<struct WeaselRecord> IterateScan() {
        it_ = db_->NewIterator(rocksdb::ReadOptions());
        struct WeaselRecord rec;
        std::vector<struct WeaselRecord> recs;

        for (it_->SeekToFirst(); it_->Valid(); it_->Next()) {
            std::string result = it_->value().ToString();
            rec.v1 = *((int*)(result.data()));
            rec.v2 = result.substr(sizeof(int), result.length() - sizeof(int));
            recs.push_back(rec);
        }

        delete it_;

        return recs;
    }*/

private:
    rocksdb::DB* db_;
    rocksdb::Options options_;
    rocksdb::Status status_;
    rocksdb::Iterator* it_;
};

int main() {
    WeaselDB db("/tmp/testdb");
    db.execute("create table planets (id int primary key, name text, moons int);");
    db.execute("insert into planets values (1, 'Mercury', 0), (2, 'Earth', 1), (3, 'Mars', 2);");
    /*
    db.execute("select id, name, moons from planets;");
    db.execute("select name, moons from planets;");
    db.execute("select name from planets where name='Earth';");
    db.execute("select name from planets where moons < 2;");
    db.execute("drop table planets;");*/
    /*
    db.InsertRecord(1, 4, "dog");
    db.InsertRecord(2, 5, "cat");
    db.InsertRecord(3, 6, "turtle");
    struct WeaselRecord result;
    if (db.GetRecord(1, &result).ok()) {
        std::cout << result.v1 << '\n' << result.v2 << std::endl;
    } else {
        std::cout << "record with key not found" << std::endl;
    }

    std::vector<struct WeaselRecord> recs = db.IterateScan();

    for (const struct WeaselRecord& r: recs) {
        std::cout << r.v1 << ", " << r.v2 << std::endl;
    }*/

}

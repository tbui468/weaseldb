#pragma once
#include <string>
#include <vector>

#include "token.h"

namespace wsldb {

class Tokenizer {
public:
    Tokenizer(const std::string& query): query_(query), idx_(0) {}
    Token NextToken();
    void DebugPrintTokens(const std::vector<Token>& tokens);
private:
    Token MakeIdentifier();
    Token MakeString();
    Token MakeNumber();

    inline Token MakeToken(TokenType type) {
        return Token(query_.substr(idx_++, 1), type);
    }

    inline void SkipWhitespace() {
        while (query_.at(idx_) == ' ' || 
               query_.at(idx_) == '\n' || 
               query_.at(idx_) == '\t') {
            idx_++;
        }
    }

    inline bool IsAlpha(char c) {
        return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_';
    }

    inline bool IsNumeric(char c) {
        return ('0' <= c && c <= '9') || c == '.';
    }

    inline bool AtEnd() {
        return idx_ >= query_.length();
    }
private:
    int idx_;
    std::string query_;
};

}

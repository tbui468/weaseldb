#include "tokenizer.h"
#include <iostream>
#include <cstring>

namespace wsldb {

Token Tokenizer::NextToken() {
    if (AtEnd()) return MakeToken(TokenType::Eof, 1);
    SkipWhitespace();
    if (AtEnd()) return MakeToken(TokenType::Eof, 1);

    if (IsAlpha(query_.at(idx_))) return MakeIdentifier();
    if (IsNumeric(query_.at(idx_))) return MakeNumber();
    if (query_.at(idx_) == '\'') return MakeString();

    switch (query_.at(idx_)) {
        case '(':
            return MakeToken(TokenType::LParen, 1);
        case ')':
            return MakeToken(TokenType::RParen, 1);
        case ',':
            return MakeToken(TokenType::Comma, 1);
        case ';':
            return MakeToken(TokenType::SemiColon, 1);
        case '=':
            return MakeToken(TokenType::Equal, 1);
        case '<': {
            char c = query_.at(idx_ + 1);
            if (c == '>') {
                return MakeToken(TokenType::NotEqual, 2);
            } else if (c == '=') {
                return MakeToken(TokenType::LessEqual, 2);
            } else {
                return MakeToken(TokenType::Less, 1);
            } 
        }
        case '>': {
            char c = query_.at(idx_ + 1);
            if (c == '=') {
                return MakeToken(TokenType::GreaterEqual, 2);
            } else {
                return MakeToken(TokenType::Greater, 1);
            }
        }
        case '+':
            return MakeToken(TokenType::Plus, 1);
        case '-':
            return MakeToken(TokenType::Minus, 1);
        case '*':
            return MakeToken(TokenType::Star, 1);
        case '/':
            return MakeToken(TokenType::Slash, 1);
        default:
            return MakeToken(TokenType::Error, 1);
    }

}

void Tokenizer::DebugPrintTokens(const std::vector<Token>& tokens) {
    for (const Token& t: tokens) {
        switch (t.type) {
            case TokenType::Identifier:
            case TokenType::IntLiteral:
            case TokenType::StringLiteral:
                std::cout << TokenTypeToString(t.type) << " " << t.lexeme << std::endl;
                break;
            default:
                std::cout << TokenTypeToString(t.type) << std::endl;
                break;
        }
    }
}

Token Tokenizer::MakeIdentifier() {
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
    } else if (len == 6 && strncmp(&query_.at(idx), "select", len) == 0) {
        type = TokenType::Select;
    } else if (len == 4 && strncmp(&query_.at(idx), "from", len) == 0) {
        type = TokenType::From;
    } else if (len == 5 && strncmp(&query_.at(idx), "where", len) == 0) {
        type = TokenType::Where;
    } else if (len == 4 && strncmp(&query_.at(idx), "bool", len) == 0) {
        type = TokenType::Bool;
    } else if (len == 4 && strncmp(&query_.at(idx), "true", len) == 0) {
        type = TokenType::TrueLiteral;
    } else if (len == 5 && strncmp(&query_.at(idx), "false", len) == 0) {
        type = TokenType::FalseLiteral;
    } else if (len == 2 && strncmp(&query_.at(idx), "or", len) == 0) {
        type = TokenType::Or;
    } else if (len == 3 && strncmp(&query_.at(idx), "and", len) == 0) {
        type = TokenType::And;
    } else if (len == 3 && strncmp(&query_.at(idx), "not", len) == 0) {
        type = TokenType::Not;
    } else if (len == 6 && strncmp(&query_.at(idx), "update", len) == 0) {
        type = TokenType::Update;
    } else if (len == 3 && strncmp(&query_.at(idx), "set", len) == 0) {
        type = TokenType::Set;
    } else if (len == 6 && strncmp(&query_.at(idx), "delete", len) == 0) {
        type = TokenType::Delete;
    } else {
        type = TokenType::Identifier;
    }

    return Token(query_.substr(idx, len), type);
}

Token Tokenizer::MakeString() {
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

Token Tokenizer::MakeNumber() {
    int idx = idx_;
    int len = 0;
    TokenType type = TokenType::IntLiteral;
    while (IsNumeric(query_.at(idx_))) {
        idx_++;
        len++;
    }
    return Token(query_.substr(idx, len), type);
}


}

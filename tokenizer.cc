#include "tokenizer.h"
#include <iostream>
#include <cstring>

namespace wsldb {

Token Tokenizer::NextToken() {
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

void Tokenizer::DebugPrintTokens(const std::vector<Token>& tokens) {
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

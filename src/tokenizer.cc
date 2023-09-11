#include "tokenizer.h"
#include <iostream>
#include <cstring>

namespace wsldb {

Token Tokenizer::NextToken() {
    if (AtEnd()) return MakeToken(TokenType::Eof, 1);
    SkipWhitespace();
    if (AtEnd()) return MakeToken(TokenType::Eof, 1);

    if (IsAlpha(query_.at(idx_))) return MakeIdentifier();
    if (IsNumeric(query_.at(idx_)) || (query_.at(idx_) == '.' && IsNumeric(query_.at(idx_ + 1)))) return MakeNumber();
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
        case '.':
            return MakeToken(TokenType::Dot, 1);
        default:
            return MakeToken(TokenType::Error, 1);
    }

}

void Tokenizer::DebugPrintTokens(const std::vector<Token>& tokens) {
    for (const Token& t: tokens) {
        switch (t.type) {
            case TokenType::Identifier:
            case TokenType::IntLiteral:
            case TokenType::FloatLiteral:
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

    while ((IsNumeric(query_.at(idx_)) || IsAlpha(query_.at(idx_))) && query_.at(idx_) != '.') {
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
    } else if (len == 4 && strncmp(&query_.at(idx), "int4", len) == 0) {
        type = TokenType::Int4;
    } else if (len == 6 && strncmp(&query_.at(idx), "float4", len) == 0) {
        type = TokenType::Float4;
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
    } else if (len == 4 && strncmp(&query_.at(idx), "null", len) == 0) {
        type = TokenType::Null;
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
    } else if (len == 4 && strncmp(&query_.at(idx), "drop", len) == 0) {
        type = TokenType::Drop;
    } else if (len == 2 && strncmp(&query_.at(idx), "if", len) == 0) {
        type = TokenType::If;
    } else if (len == 6 && strncmp(&query_.at(idx), "exists", len) == 0) {
        type = TokenType::Exists;
    } else if (len == 7 && strncmp(&query_.at(idx), "foreign", len) == 0) {
        type = TokenType::Foreign;
    } else if (len == 10 && strncmp(&query_.at(idx), "references", len) == 0) {
        type = TokenType::References;
    } else if (len == 8 && strncmp(&query_.at(idx), "describe", len) == 0) {
        type = TokenType::Describe;
    } else if (len == 5 && strncmp(&query_.at(idx), "order", len) == 0) {
        type = TokenType::Order;
    } else if (len == 2 && strncmp(&query_.at(idx), "by", len) == 0) {
        type = TokenType::By;
    } else if (len == 4 && strncmp(&query_.at(idx), "desc", len) == 0) {
        type = TokenType::Desc;
    } else if (len == 3 && strncmp(&query_.at(idx), "asc", len) == 0) {
        type = TokenType::Asc;
    } else if (len == 5 && strncmp(&query_.at(idx), "limit", len) == 0) {
        type = TokenType::Limit;
    } else if (len == 5 && strncmp(&query_.at(idx), "group", len) == 0) {
        type = TokenType::Group;
    } else if (len == 3 && strncmp(&query_.at(idx), "avg", len) == 0) {
        type = TokenType::Avg;
    } else if (len == 5 && strncmp(&query_.at(idx), "count", len) == 0) {
        type = TokenType::Count;
    } else if (len == 3 && strncmp(&query_.at(idx), "sum", len) == 0) {
        type = TokenType::Sum;
    } else if (len == 3 && strncmp(&query_.at(idx), "max", len) == 0) {
        type = TokenType::Max;
    } else if (len == 3 && strncmp(&query_.at(idx), "min", len) == 0) {
        type = TokenType::Min;
    } else if (len == 8 && strncmp(&query_.at(idx), "distinct", len) == 0) {
        type = TokenType::Distinct;
    } else if (len == 2 && strncmp(&query_.at(idx), "as", len) == 0) {
        type = TokenType::As;
    } else if (len == 5 && strncmp(&query_.at(idx), "cross", len) == 0) {
        type = TokenType::Cross;
    } else if (len == 4 && strncmp(&query_.at(idx), "join", len) == 0) {
        type = TokenType::Join;
    } else if (len == 2 && strncmp(&query_.at(idx), "on", len) == 0) {
        type = TokenType::On;
    } else if (len == 5 && strncmp(&query_.at(idx), "inner", len) == 0) {
        type = TokenType::Inner;
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
    while (IsNumeric(query_.at(idx_)) || query_.at(idx_) == '.') {
        if (query_.at(idx_) == '.')
            type = TokenType::FloatLiteral;
        idx_++;
        len++;
    }
    return Token(query_.substr(idx, len), type);
}


}

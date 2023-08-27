#include "token.h"


namespace wsldb {

std::string TokenTypeToString(TokenType type) {
    switch (type) {
        case TokenType::Create:
            return "Create";
        case TokenType::Table:
            return "Table";
        case TokenType::Text:
            return "Text";
        case TokenType::Int:
            return "Int";
        case TokenType::Primary:
            return "Primary";
        case TokenType::Key:
            return "Key";
        case TokenType::Insert:
            return "Insert";
        case TokenType::Into:
            return "Into";
        case TokenType::Values:
            return "Values";
        case TokenType::Identifier:
            return "Identifier";
        case TokenType::IntLiteral:
            return "IntLiteral";
        case TokenType::StringLiteral:
            return "StringLiteral";
        case TokenType::LParen:
            return "LParen";
        case TokenType::RParen:
            return "RParen";
        case TokenType::Comma:
            return "Comma";
        case TokenType::SemiColon:
            return "SemiColon";
        case TokenType::Eof:
            return "Eof";
        case TokenType::Error:
            return "Error";
        default:
            return "Unrecognized token";
    }
}

}

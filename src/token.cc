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
        case TokenType::Select:
            return "Select";
        case TokenType::From:
            return "From";
        case TokenType::Where:
            return "Where";
        case TokenType::Equal:
            return "Equal";
        case TokenType::NotEqual:
            return "NotEqual";
        case TokenType::Less:
            return "Less";
        case TokenType::Greater:
            return "Greater";
        case TokenType::LessEqual:
            return "LessEqual";
        case TokenType::GreaterEqual:
            return "GreaterEqual";
        case TokenType::Bool:
            return "Bool";
        case TokenType::TrueLiteral:
            return "TrueLiteral";
        case TokenType::FalseLiteral:
            return "FalseLiteral";
        case TokenType::Plus:
            return "Plus";
        case TokenType::Minus:
            return "Minus";
        case TokenType::Star:
            return "Star";
        case TokenType::Slash:
            return "Slash";
        case TokenType::And:
            return "And";
        case TokenType::Or:
            return "Or";
        case TokenType::Not:
            return "Not";
        case TokenType::Update:
            return "Update";
        case TokenType::Set:
            return "Set";
        case TokenType::Delete:
            return "Delete";
        case TokenType::Drop:
            return "Drop";
        case TokenType::If:
            return "If";
        case TokenType::Exists:
            return "Exists";
        case TokenType::Foreign:
            return "Foreign";
        case TokenType::References:
            return "References";
        case TokenType::Describe:
            return "Describe";
        default:
            return "Unrecognized token";
    }
}

bool TokenTypeIsNumeric(TokenType type) {
    return type == TokenType::Int; //TODO: add more types here
}

}

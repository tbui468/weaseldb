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
        case TokenType::Int8:
            return "Int8";
        case TokenType::Float4:
            return "Float4";
        case TokenType::Bytea:
            return "Bytea";
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
        case TokenType::Null:
            return "Null";
        case TokenType::TrueLiteral:
            return "TrueLiteral";
        case TokenType::FalseLiteral:
            return "FalseLiteral";
        case TokenType::ByteaLiteral:
            return "ByteaLiteral";
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
        case TokenType::Order:
            return "Order";
        case TokenType::By:
            return "By";
        case TokenType::Desc:
            return "Desc";
        case TokenType::Asc:
            return "Asc";
        case TokenType::Limit:
            return "Limit";
        case TokenType::Group:
            return "Group";
        case TokenType::Count:
            return "Count";
        case TokenType::Avg:
            return "Avg";
        case TokenType::Sum:
            return "Sum";
        case TokenType::Max:
            return "Max";
        case TokenType::Min:
            return "Min";
        case TokenType::Distinct:
            return "Distinct";
        case TokenType::As:
            return "As";
        case TokenType::Dot:
            return "Dot";
        case TokenType::Cross:
            return "Cross";
        case TokenType::Join:
            return "Join";
        case TokenType::On:
            return "On";
        case TokenType::Inner:
            return "Inner";
        case TokenType::Is:
            return "Is";
        case TokenType::Left:
            return "Left";
        case TokenType::Right:
            return "Right";
        case TokenType::Full:
            return "Full";
        case TokenType::Unique:
            return "Unique";
        case TokenType::Nulls:
            return "Nulls";
        default:
            return "Unrecognized token";
    }
}

DatumType LiteralTokenToDatumType(TokenType type) {
    switch (type) {
        case TokenType::IntLiteral:
            return DatumType::Int8;
        case TokenType::FloatLiteral:
            return DatumType::Float4;
        case TokenType::StringLiteral:
            return DatumType::Text;
        case TokenType::TrueLiteral:
        case TokenType::FalseLiteral:
            return DatumType::Bool;
        case TokenType::ByteaLiteral:
            return DatumType::Bytea;
        case TokenType::Null:
            return DatumType::Null;
        default:
            return DatumType::Null;
    }
}

DatumType TypeTokenToDatumType(TokenType type) {
    switch (type) {
        case TokenType::Int8:
            return DatumType::Int8;
        case TokenType::Float4:
            return DatumType::Float4;
        case TokenType::Text:
            return DatumType::Text;
        case TokenType::Bool:
            return DatumType::Bool;
        case TokenType::Bytea:
            return DatumType::Bytea;
        default:
            return DatumType::Null;
    }
}

bool TokenTypeValidDataType(TokenType type) {
    return type == TokenType::Int8 ||
           type == TokenType::Float4 ||
           type == TokenType::Text ||
           type == TokenType::Bool ||
           type == TokenType::Bytea;
}

std::vector<TokenType> TokenTypeSQLDataTypes() {
    return { TokenType::Int8, 
             TokenType::Float4,
             TokenType::Text,
             TokenType::Bool,
             TokenType::Bytea };
}

bool TokenTypeIsAggregateFunction(TokenType type) {
    return type == TokenType::Avg ||
           type == TokenType::Sum ||
           type == TokenType::Count ||
           type == TokenType::Max ||
           type == TokenType::Min;
}

bool TokensSubsetOf(const std::vector<Token>& left, const std::vector<Token>& right) {
    for (Token t: left) {
        if (!TokenIn(t, right))
            return false;
    }
    return true;
}

bool TokenIn(Token left, const std::vector<Token>& right) {
    for (Token t: right) {
        if (left.lexeme.compare(t.lexeme) == 0)
            return true;
    }
    return false;
}

}

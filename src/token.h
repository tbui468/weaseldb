#pragma once

#include <string>
#include <vector>

namespace wsldb {

enum class TokenType {
    /* SQL keywords */
    Create,
    Table, 
    Text,
    Int4,
    Int8,
    Bool,
    Float4,
    Null,
    Primary,
    Key,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    Update,
    Set,
    Delete,
    Drop,
    If,
    Exists,
    Foreign,
    References,
    Describe,
    Order,
    By,
    Desc,
    Asc,
    Limit,
    Group,
    Count,
    Avg,
    Sum,
    Min,
    Max,
    Distinct,
    As,
    Cross,
    Join,
    On,
    Inner,
    Is,
    Left,
    Right,
    Full,
    Unique,
    Nulls,

    /* user-defined identifier */
    Identifier,

    /* literals */
    IntLiteral,
    FloatLiteral,
    StringLiteral,
    TrueLiteral,
    FalseLiteral,

    /* single characters */
    LParen,
    RParen,
    Comma,
    SemiColon,
    Eof,
    Equal,
    NotEqual,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,
    Plus,
    Minus,
    Star,
    Slash,
    Dot,

    /* error */
    Error
};

std::string TokenTypeToString(TokenType type);
bool TokenTypeIsNumeric(TokenType type);
bool TokenTypeIsInteger(TokenType type);
bool TokenTypeValidDataType(TokenType type);
bool TokenTypeIsAggregateFunction(TokenType type);

class Token {
public:
    Token(std::string lexeme, TokenType type): lexeme(std::move(lexeme)), type(type) {}
    std::string lexeme;
    TokenType type;
};

bool TokensSubsetOf(const std::vector<Token>& left, const std::vector<Token>& right);
bool TokenIn(Token left, const std::vector<Token>& right);

}

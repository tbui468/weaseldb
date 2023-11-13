#pragma once

#include <string>
#include <vector>
#include "datum.h"

namespace wsldb {

enum class TokenType {
    /* SQL keywords */
    Create,
    Table, 
    Text,
    Int8,
    Bool,
    Float4,
    Bytea,
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
    Begin,
    Commit,
    Rollback,
    Model,

    /* user-defined identifier */
    Identifier,

    /* literals */
    IntLiteral,
    FloatLiteral,
    StringLiteral,
    TrueLiteral,
    FalseLiteral,
    ByteaLiteral,

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
    Dot
};

std::string TokenTypeToString(TokenType type);
DatumType LiteralTokenToDatumType(TokenType type);
DatumType TypeTokenToDatumType(TokenType type);
bool TokenTypeValidDataType(TokenType type);
std::vector<TokenType> TokenTypeSQLDataTypes();
bool TokenTypeIsAggregateFunction(TokenType type);

class Token {
public:
    Token(): lexeme(""), type(TokenType::Null) {}
    Token(std::string lexeme, TokenType type): lexeme(std::move(lexeme)), type(type) {}
    std::string lexeme;
    TokenType type;
};

bool TokensSubsetOf(const std::vector<Token>& left, const std::vector<Token>& right);
bool TokenIn(Token left, const std::vector<Token>& right);

}

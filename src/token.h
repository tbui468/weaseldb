#pragma once

#include <string>

namespace wsldb {

enum class TokenType {
    /* SQL keywords */
    Create,
    Table, 
    Text,
    Int,
    Bool,
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

    /* user-defined identifier */
    Identifier,

    /* literals */
    IntLiteral,
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

    /* error */
    Error
};

std::string TokenTypeToString(TokenType type);
bool TokenTypeIsNumeric(TokenType type);

class Token {
public:
    Token(std::string lexeme, TokenType type): lexeme(std::move(lexeme)), type(type) {}
    std::string lexeme;
    TokenType type;
};

}

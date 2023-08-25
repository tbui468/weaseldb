#pragma once

#include <string>

namespace wsldb {

enum class TokenType {
    /* SQL keywords */
    Create,
    Table, 
    Text,
    Int,
    Primary,
    Key,
    Insert,
    Into,
    Values,

    /* user-defined identifier */
    Identifier,

    /* literals */
    IntLiteral,
    StringLiteral,

    /* single characters */
    LParen,
    RParen,
    Comma,
    SemiColon,
    Eof,

    /* error */
    Error
};

class Token {
public:
    Token(std::string lexeme, TokenType type): lexeme(std::move(lexeme)), type(type) {}
    std::string lexeme;
    TokenType type;
};

}

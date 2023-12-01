#include "tokenizer.h"
#include <cstring>
#include <cctype>
#include <algorithm>

namespace wsldb {


struct Keyword {
    const char* string;
    TokenType type;
};

//this should also go in tokenizer.cc
static std::vector<Keyword> keywords[] = {
    {}, //len == 0
    {}, //1
    {   //2
        {"or", TokenType::Or},
        {"if", TokenType::If},
        {"by", TokenType::By},
        {"as", TokenType::As},
        {"is", TokenType::Is},
        {"on", TokenType::On},
        {"to", TokenType::To}
    },
    {   //3
        {"key", TokenType::Key},
        {"and", TokenType::And},
        {"not", TokenType::Not},
        {"set", TokenType::Set},
        {"asc", TokenType::Asc},
        {"avg", TokenType::Avg},
        {"sum", TokenType::Sum},
        {"max", TokenType::Max},
        {"min", TokenType::Min}
    },
    {   //4
        {"text", TokenType::Text},
        {"into", TokenType::Into},
        {"from", TokenType::From},
        {"bool", TokenType::Bool},
        {"null", TokenType::Null},
        {"true", TokenType::TrueLiteral},
        {"drop", TokenType::Drop},
        {"desc", TokenType::Desc},
        {"join", TokenType::Join},
        {"left", TokenType::Left},
        {"full", TokenType::Full},
        {"int8", TokenType::Int8},
        {"cast", TokenType::Cast},
        {"like", TokenType::Like}
    },
    {   //5
        {"table", TokenType::Table},
        {"where", TokenType::Where},
        {"bytea", TokenType::Bytea},
        {"false", TokenType::FalseLiteral},
        {"order", TokenType::Order},
        {"limit", TokenType::Limit},
        {"group", TokenType::Group},
        {"count", TokenType::Count},
        {"cross", TokenType::Cross},
        {"inner", TokenType::Inner},
        {"right", TokenType::Right},
        {"nulls", TokenType::Nulls},
        {"begin", TokenType::Begin},
        {"model", TokenType::Model}
    },
    {   //6
        {"create", TokenType::Create},
        {"float4", TokenType::Float4},
        {"insert", TokenType::Insert},
        {"values", TokenType::Values},
        {"select", TokenType::Select},
        {"update", TokenType::Update},
        {"delete", TokenType::Delete},
        {"exists", TokenType::Exists},
        {"unique", TokenType::Unique},
        {"commit", TokenType::Commit},
        {"having", TokenType::Having}
    },
    {   //7
        {"primary", TokenType::Primary},
        {"foreign", TokenType::Foreign},
        {"similar", TokenType::Similar}
    },
    {   //8
        {"describe", TokenType::Describe},
        {"distinct", TokenType::Distinct},
        {"rollback", TokenType::Rollback}
    },
    {   //9
        {"timestamp", TokenType::Timestamp}
    },
    {   //10
        {"references", TokenType::References}
    }
};

Status Tokenizer::NextToken(Token* t) {
    if (AtEnd()) return MakeToken(t, TokenType::Eof, 1);
    SkipWhitespace();
    if (AtEnd()) return MakeToken(t, TokenType::Eof, 1);

    if (IsAlpha(query_.at(idx_))) return MakeIdentifier(t);
    if (IsNumeric(query_.at(idx_)) || (query_.at(idx_) == '.' && IsNumeric(query_.at(idx_ + 1)))) return MakeNumber(t);
    if (query_.at(idx_) == '\'') return MakeString(t);

    switch (query_.at(idx_)) {
        case '(':
            return MakeToken(t, TokenType::LParen, 1);
        case ')':
            return MakeToken(t, TokenType::RParen, 1);
        case ',':
            return MakeToken(t, TokenType::Comma, 1);
        case ';':
            return MakeToken(t, TokenType::SemiColon, 1);
        case '=':
            return MakeToken(t, TokenType::Equal, 1);
        case '<': {
            char c = query_.at(idx_ + 1);
            if (c == '>') {
                return MakeToken(t, TokenType::NotEqual, 2);
            } else if (c == '=') {
                return MakeToken(t, TokenType::LessEqual, 2);
            } else {
                return MakeToken(t, TokenType::Less, 1);
            } 
        }
        case '>': {
            char c = query_.at(idx_ + 1);
            if (c == '=') {
                return MakeToken(t, TokenType::GreaterEqual, 2);
            } else {
                return MakeToken(t, TokenType::Greater, 1);
            }
        }
        case '+':
            return MakeToken(t, TokenType::Plus, 1);
        case '-': {
            char c = query_.at(idx_ + 1);
            if (c == '-') {
                while (!AtEnd() && query_.at(idx_) != '\n')
                    idx_++;
                return NextToken(t);
            }
            return MakeToken(t, TokenType::Minus, 1);
        }
        case '*':
            return MakeToken(t, TokenType::Star, 1);
        case '/': {
            char c = query_.at(idx_ + 1);
            if (c == '*') {
                SkipUntil("*/");
                return NextToken(t);
            }
            return MakeToken(t, TokenType::Slash, 1);
        }
        case '.':
            return MakeToken(t, TokenType::Dot, 1);
        default:
            return Status(false, "Error: Invalid token '" + std::string(1, query_.at(idx_)) + "'");
    }

}

std::string Tokenizer::DebugTokensToString(const std::vector<Token>& tokens) {
    std::string result = "";
    for (const Token& t: tokens) {
        switch (t.type) {
            case TokenType::Identifier:
            case TokenType::IntLiteral:
            case TokenType::FloatLiteral:
            case TokenType::StringLiteral:
            case TokenType::ByteaLiteral:
                result += TokenTypeToString(t.type) + " " + t.lexeme + "\n";
                break;
            default:
                result += TokenTypeToString(t.type) + "\n";
                break;
        }
    }

    return result;
}

Status Tokenizer::MakeIdentifier(Token* t) {
    size_t idx = idx_;
    size_t len = 0;

    while ((IsNumeric(query_.at(idx_)) || IsAlpha(query_.at(idx_))) && query_.at(idx_) != '.') {
        idx_++;
        len++;
    }

    std::string s = query_.substr(idx, len);
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });

    TokenType type = TokenType::Identifier;

    if (len < std::size(keywords)) {
        for (const Keyword& kw: keywords[len]) {
            if (strncmp(s.data(), kw.string, len) == 0)
                type = kw.type;
        }
    }
    *t = Token(s, type);
    return Status();
}

Status Tokenizer::MakeString(Token* t) {
    idx_++; //skip starting single quote
    size_t start_idx = idx_;
    size_t len = 0;
    TokenType type = TokenType::StringLiteral;

    //single quotes are used as an escape character, so need to skip ''
    while (true) {
        char next = query_.at(idx_);
        if (next == '\'' && idx_ + 1 < query_.length() && query_.at(idx_ + 1) != '\'') {
            idx_++;
            break;
        } else if (next == '\'' && idx_ + 1 < query_.length() && query_.at(idx_ + 1) == '\'') {
            idx_ += 2;
            len += 2;
        } else {
            idx_++;
            len++;
        }
    }

    //escape any two single quotes found
    std::string s = query_.substr(start_idx, len);
    Tokenizer::ReplaceAll(s, "\'\'", "\'");

    //if first two characters are \x, must be a bytea literal
    if (s.at(0) == '\\' && s.size() > 1 && s.at(1) == 'x') {
        type = TokenType::ByteaLiteral;
    }

    *t = Token(s, type);
    return Status();
}

Status Tokenizer::MakeNumber(Token* t) {
    size_t idx = idx_;
    size_t len = 0;
    TokenType type = TokenType::IntLiteral;
    while (IsNumeric(query_.at(idx_)) || query_.at(idx_) == '.') {
        if (query_.at(idx_) == '.')
            type = TokenType::FloatLiteral;
        idx_++;
        len++;
    }
    *t = Token(query_.substr(idx, len), type);
    return Status();
}


}

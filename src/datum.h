#pragma once

#include <string>
#include "token.h"

namespace wsldb {

class Datum {
public:
    Datum(Token t) {
        switch (t.type) {
            case TokenType::IntLiteral: {
                type_ = TokenType::Int;
                int value = std::stoi(t.lexeme);
                data_.append((char*)&value, sizeof(int));
                break;
            }
            case TokenType::StringLiteral: {
                type_ = TokenType::Text;
                int size = t.lexeme.size();
                data_.append((char*)&size, sizeof(int));
                data_ += t.lexeme;
                break;
            }
            case TokenType::TrueLiteral: {
                type_ = TokenType::Bool;
                bool value = true;
                data_.append((char*)&value, sizeof(bool));
                break;
            }
            case TokenType::FalseLiteral: {
                type_ = TokenType::Bool;
                bool value = false;
                data_.append((char*)&value, sizeof(bool));
                break;
            }
            default:
                std::cout << "invalid token type for initializing datum: " << TokenTypeToString(t.type) << std::endl;
                break;
        }
    }

    Datum(const std::string& buf, int* off, TokenType type) {
        type_ = type;
        switch (type) {
            case TokenType::Int: {
                data_.append((char*)(buf.data() + *off), sizeof(int));
                *off += sizeof(int);
                break;
            }
            case TokenType::Text: {
                int size = *((int*)(buf.data() + *off));
                *off += sizeof(int);
                data_.append((char*)&size, sizeof(int));
                data_.append(buf.data() + *off, size);
                *off += size;
                break;
            }
            case TokenType::Bool: {
                data_.append((char*)(buf.data() + *off), sizeof(bool));
                *off += sizeof(bool);
                break;
            }
            default:
                std::cout << "invalid buffer data for initializing datum: " << TokenTypeToString(type) << std::endl;
                break;
        } 
    }

    TokenType Type() const {
        return type_;
    }

    std::string ToString() const {
        return data_;
    }

    std::string AsString() {
        int off = sizeof(int); //string length
        return data_.substr(off, data_.size() - off);
    }

    int AsInt() {
        return *((int*)(data_.data()));
    }

    bool AsBool() {
        return *((bool*)(data_.data()));
    }
private:
    TokenType type_;
    std::string data_;
};

}

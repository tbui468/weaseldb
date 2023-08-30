#pragma once

#include <string>
#include <iostream>
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

    Datum(int i) {
        type_ = TokenType::Int;
        data_.append((char*)&i, sizeof(int));
    }

    Datum(bool b) {
        type_ = TokenType::Bool;
        data_.append((char*)&b, sizeof(bool));
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

    Datum Compare(Datum& d) {
        switch (type_) {
            case TokenType::Int:
                return Datum(AsInt() - d.AsInt());
            case TokenType::Bool:
                return Datum(AsBool() - d.AsBool());
            case TokenType::Text:
                return Datum(AsString().compare(d.AsString()));
            default:
                std::cout << "invalid type for comparing data: " << TokenTypeToString(type_) << std::endl;
                break;
        }        
        return Datum(0);
    }
private:
    TokenType type_;
    std::string data_;
};

}

#pragma once

#include <string>
#include <iostream>
#include <limits>
#include "token.h"

namespace wsldb {

#define WSLDB_NUMERIC_LITERAL(d) ((d).Type() == TokenType::Float4 ? (d).AsFloat4() : (d).Type() == TokenType::Int4 ? (d).AsInt4() : (d).AsInt8())
#define WSLDB_INTEGER_LITERAL(d) ((d).Type() == TokenType::Int4 ? (d).AsInt4() : (d).AsInt8())


class Datum {
public:
    Datum(Token t) {
        switch (t.type) {
            case TokenType::IntLiteral: {
                //all integer literals are evaluated to int8 for processing in wsldb
                //if schema expects int4, will be demoted to int4 before written to disk inside of class ColAssign
                type_ = TokenType::Int8;
                int64_t value = std::stol(t.lexeme);
                data_.append((char*)&value, sizeof(int64_t));
                break;
            }
            case TokenType::FloatLiteral: {
                type_ = TokenType::Float4;
                float value = std::stof(t.lexeme);
                data_.append((char*)&value, sizeof(float));
                break;
            }
            case TokenType::StringLiteral: {
                type_ = TokenType::Text;
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
            case TokenType::Null: {
                type_ = TokenType::Null;
                break;
            }
            default:
                //TODO: this should report and error rather than printing 
                std::cout << "invalid token type for initializing datum: " << TokenTypeToString(t.type) << std::endl;
                break;
        }
    }

    Datum(const std::string& buf, int* off, TokenType type) {
        bool is_null = *((bool*)(buf.data() + *off));
        *off += sizeof(bool);

        if (is_null) {
            type_ = TokenType::Null;
            data_ = "";
            return;
        }

        type_ = type;
        switch (type) {
            case TokenType::Int4: {
                data_.append((char*)(buf.data() + *off), sizeof(int));
                *off += sizeof(int);
                break;
            }
            case TokenType::Int8: {
                data_.append((char*)(buf.data() + *off), sizeof(int64_t));
                *off += sizeof(int64_t);
                break;
            }
            case TokenType::Float4: {
                data_.append((char*)(buf.data() + *off), sizeof(float));
                *off += sizeof(float);
                break;
            }
            case TokenType::Text: {
                int size = *((int*)(buf.data() + *off));
                *off += sizeof(int);
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

    Datum() {
        type_ = TokenType::Null;
        data_ = "";
    }

    //TODO: change this to int32_t, and fix any warnings/errors that occur in program
    Datum(int i) {
        type_ = TokenType::Int4;
        data_.append((char*)&i, sizeof(int));
    }

    Datum(int64_t i) {
        type_ = TokenType::Int8;
        data_.append((char*)&i, sizeof(int64_t));
    }

    Datum(float f) {
        type_ = TokenType::Float4;
        data_.append((char*)&f, sizeof(float));
    }

    Datum(bool b) {
        type_ = TokenType::Bool;
        data_.append((char*)&b, sizeof(bool));
    }

    Datum(const std::string& s) {
        type_ = TokenType::Text;
        data_ = s;
    }

    TokenType Type() const {
        return type_;
    }

    std::string Serialize() const {
        std::string result;
        bool is_null = false;
        if (type_ == TokenType::Null) {
            is_null = true;
        }

        result.append((char*)&is_null, sizeof(bool));

        if (type_ == TokenType::Text) {
            int size = data_.size();
            result.append((char*)&size, sizeof(int));
        }
            
        result += data_;

        return result;
    }

    std::string AsString() const {
        return data_;
    }

    int AsInt4() const {
        return *((int*)(data_.data()));
    }

    int64_t AsInt8() const {
        return *((int64_t*)(data_.data()));
    }

    float AsFloat4() const {
        return *((float*)(data_.data()));
    }

    bool AsBool() const {
        return *((bool*)(data_.data()));
    }

    Datum operator+(const Datum& d) {
        return Datum(WSLDB_NUMERIC_LITERAL(*this) + WSLDB_NUMERIC_LITERAL(d));
    }

    Datum operator+=(const Datum& d) {
        *this = *this + d;
        return *this;
    }

    Datum operator-(const Datum& d) {
        return Datum(WSLDB_NUMERIC_LITERAL(*this) - WSLDB_NUMERIC_LITERAL(d));
    }

    Datum operator-=(const Datum& d) {
        *this = *this - d;
        return *this;
    }

    Datum operator*(const Datum& d) {
        return Datum(WSLDB_NUMERIC_LITERAL(*this) * WSLDB_NUMERIC_LITERAL(d));
    }

    Datum operator*=(const Datum& d) {
        *this = *this * d;
        return *this;
    }

    Datum operator/(const Datum& d) {
        return Datum(WSLDB_NUMERIC_LITERAL(*this) / WSLDB_NUMERIC_LITERAL(d));
    }

    Datum operator/=(const Datum& d) {
        *this = *this / d;
        return *this;
    }

    bool operator==(const Datum& d) {
        switch (type_) {
            case TokenType::Int4:
            case TokenType::Int8:
            case TokenType::Float4:
                return WSLDB_NUMERIC_LITERAL(*this) - WSLDB_NUMERIC_LITERAL(d) == 0;
            case TokenType::Bool:
                return AsBool() - d.AsBool() == 0;
            case TokenType::Text:
                return AsString().compare(d.AsString()) == 0;
            default:
                std::cout << "invalid type for comparing data: " << TokenTypeToString(type_) << std::endl;
                break;
        }        
        return false;
    }

    bool operator!=(const Datum& d) {
        return !(*this == d);
    }

    bool operator<(const Datum& d) {
        switch (type_) {
            case TokenType::Int4:
            case TokenType::Int8:
            case TokenType::Float4:
                return WSLDB_NUMERIC_LITERAL(*this) - WSLDB_NUMERIC_LITERAL(d) < 0;
            case TokenType::Bool:
                return AsBool() - d.AsBool() < 0;
            case TokenType::Text:
                return AsString().compare(d.AsString()) < 0;
            default:
                std::cout << "invalid type for comparing data: " << TokenTypeToString(type_) << std::endl;
                break;
        }        
        return false;
    }

    bool operator<=(const Datum& d) {
        return *this == d || *this < d;
    }

    bool operator>=(const Datum& d) {
        return !(*this < d);
    }

    bool operator>(const Datum& d) {
        return !(*this <= d);
    }

    bool operator||(const Datum& d) {
        return (*this).AsBool() || d.AsBool();
    }

    bool operator&&(const Datum& d) {
        return (*this).AsBool() && d.AsBool();
    }

    static std::string SerializeData(const std::vector<Datum>& data) {
        std::string value;
        for (Datum d: data) {
            value += d.Serialize();
        }
        return value;
    }

private:
    TokenType type_;
    std::string data_;
};

}

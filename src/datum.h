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
                type_ = TokenType::Int4;
                int value = std::stoi(t.lexeme);
                data_.append((char*)&value, sizeof(int));
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

    Datum(int i) {
        type_ = TokenType::Int4;
        data_.append((char*)&i, sizeof(int));
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

    float AsFloat4() const {
        return *((float*)(data_.data()));
    }

    bool AsBool() const {
        return *((bool*)(data_.data()));
    }

    Datum operator+(const Datum& d) {
        return Datum((Type() == TokenType::Int4 ? AsInt4() : AsFloat4()) + 
                     (d.Type() == TokenType::Int4 ? d.AsInt4() : d.AsFloat4()));
    }

    Datum operator+=(const Datum& d) {
        *this = *this + d;
        return *this;
    }

    Datum operator-(const Datum& d) {
        return Datum((Type() == TokenType::Int4 ? AsInt4() : AsFloat4()) -
                     (d.Type() == TokenType::Int4 ? d.AsInt4() : d.AsFloat4()));
    }

    Datum operator-=(const Datum& d) {
        *this = *this - d;
        return *this;
    }

    Datum operator*(const Datum& d) {
        return Datum((Type() == TokenType::Int4 ? AsInt4() : AsFloat4()) *
                     (d.Type() == TokenType::Int4 ? d.AsInt4() : d.AsFloat4()));
    }

    Datum operator*=(const Datum& d) {
        *this = *this * d;
        return *this;
    }

    Datum operator/(const Datum& d) {
        return Datum((Type() == TokenType::Int4 ? AsInt4() : AsFloat4()) /
                     (d.Type() == TokenType::Int4 ? d.AsInt4() : d.AsFloat4()));
    }

    Datum operator/=(const Datum& d) {
        *this = *this / d;
        return *this;
    }

    bool operator==(const Datum& d) {
        return this->Compare(d).AsInt4() == 0;
    }

    bool operator!=(const Datum& d) {
        return !(*this == d);
    }

    bool operator<(const Datum& d) {
        return this->Compare(d).AsInt4() < 0;
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
    Datum Compare(const Datum& d) {
        switch (type_) {
            case TokenType::Int4:
                return Datum(AsInt4() - d.AsInt4());
            case TokenType::Float4:
                return Datum(AsFloat4() - d.AsFloat4());
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

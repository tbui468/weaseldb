#pragma once

#include <string>
#include <vector>

namespace wsldb {

#define WSLDB_NUMERIC_LITERAL(d) ((d).IsType(DatumType::Float4) ? (d).AsFloat4() : (d).AsInt8())

enum class DatumType {
    Int8,
    Float4,
    Text,
    Bool,
    Null,
    Bytea
};

class Datum {
public:
    Datum(DatumType type, const std::string& lexeme) {
        type_ = type;
        switch (type) {
            case DatumType::Int8: {
                int64_t value = std::stol(lexeme);
                data_.append((char*)&value, sizeof(int64_t));
                break;
            }
            case DatumType::Float4: {
                float value = std::stof(lexeme);
                data_.append((char*)&value, sizeof(float));
                break;
            }
            case DatumType::Text: {
                data_ += lexeme;
                break;
            }
            case DatumType::Bytea: {
                for (size_t i = 2 /*skip \x*/; i < lexeme.size(); i += 2) {
                    std::string pair = lexeme.substr(i, 2);
                    char byte = (char)strtol(pair.c_str(), NULL, 16);
                    data_.append((char*)&byte, sizeof(char));
                }
                break;
            }
            case DatumType::Bool: {
                bool value = false;
                if (lexeme.compare("true") == 0) {
                    value = true;
                }
                data_.append((char*)&value, sizeof(bool));
                break;
            }
            case DatumType::Null: {
                break;
            }
            default:
                //TODO: this should report and error rather than printing 
                //std::cout << "invalid token type for initializing datum: " << TokenTypeToString(t.type) << std::endl;
                break;
        }
    }

    //type is the expected datum type based on schema, not the actual type (which may be a null)
    Datum(const std::string& buf, int* off, DatumType type) {
        bool is_null = *((bool*)(buf.data() + *off));
        *off += sizeof(bool);

        if (is_null) {
            type_ = DatumType::Null;
            data_ = "";
            return;
        }

        type_ = type;
        switch (type) {
            case DatumType::Int8: {
                data_.append((char*)(buf.data() + *off), sizeof(int64_t));
                *off += sizeof(int64_t);
                break;
            }
            case DatumType::Float4: {
                data_.append((char*)(buf.data() + *off), sizeof(float));
                *off += sizeof(float);
                break;
            }
            case DatumType::Bytea:
            case DatumType::Text: {
                int size = *((int*)(buf.data() + *off));
                *off += sizeof(int);
                data_.append(buf.data() + *off, size);
                *off += size;
                break;
            }
            case DatumType::Bool: {
                data_.append((char*)(buf.data() + *off), sizeof(bool));
                *off += sizeof(bool);
                break;
            }
            default:
                //std::cout << "invalid buffer data for initializing datum: " << TokenTypeToString(type) << std::endl;
                break;
        } 
    }

    Datum() {
        type_ = DatumType::Null;
        data_ = "";
    }

    Datum(int i): Datum(static_cast<int64_t>(i)) {}

    Datum(int64_t i) {
        type_ = DatumType::Int8;
        data_.append((char*)&i, sizeof(int64_t));
    }

    Datum(float f) {
        type_ = DatumType::Float4;
        data_.append((char*)&f, sizeof(float));
    }

    Datum(bool b) {
        type_ = DatumType::Bool;
        data_.append((char*)&b, sizeof(bool));
    }

    Datum(const std::string& s) {
        type_ = DatumType::Text;
        data_ = s;
    }

    bool IsType(DatumType type) const {
        return type_ == type;
    }

    DatumType Type() const {
        return type_;
    }

    std::string Serialize() const {
        std::string result;
        bool is_null = false;

        //if null, don't include data (since it won't be valid anyway)
        if (type_ == DatumType::Null) {
            is_null = true;
            result.append((char*)&is_null, sizeof(bool));
            return result;
        }

        result.append((char*)&is_null, sizeof(bool));

        if (type_ == DatumType::Text || type_ == DatumType::Bytea) {
            int size = data_.size();
            result.append((char*)&size, sizeof(int));
        }
            
        result += data_;

        return result;
    }

    std::string AsString() const {
        return data_;
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

    std::string AsBytea() const {
        return data_;
    }

    Datum operator+(const Datum& d) {
        if (TypeIsInteger(this->Type()) && TypeIsInteger(d.Type())) {
            return Datum(static_cast<int64_t>(this->AsInt8() + d.AsInt8()));
        }

        return Datum(static_cast<float>(WSLDB_NUMERIC_LITERAL(*this) + WSLDB_NUMERIC_LITERAL(d)));
    }

    Datum operator+=(const Datum& d) {
        *this = *this + d;
        return *this;
    }

    Datum operator-(const Datum& d) {
        if (TypeIsInteger(this->Type()) && TypeIsInteger(d.Type())) {
            return Datum(static_cast<int64_t>(this->AsInt8() - d.AsInt8()));
        }

        return Datum(static_cast<float>(WSLDB_NUMERIC_LITERAL(*this) - WSLDB_NUMERIC_LITERAL(d)));
    }

    Datum operator-=(const Datum& d) {
        *this = *this - d;
        return *this;
    }

    Datum operator*(const Datum& d) {
        if (TypeIsInteger(this->Type()) && TypeIsInteger(d.Type())) {
            return Datum(static_cast<int64_t>(this->AsInt8() * d.AsInt8()));
        }

        return Datum(static_cast<float>(WSLDB_NUMERIC_LITERAL(*this) * WSLDB_NUMERIC_LITERAL(d)));
    }

    Datum operator*=(const Datum& d) {
        *this = *this * d;
        return *this;
    }

    Datum operator/(const Datum& d) {
        if (TypeIsInteger(this->Type()) && TypeIsInteger(d.Type())) {
            return Datum(static_cast<int64_t>(this->AsInt8() / d.AsInt8()));
        }

        return Datum(static_cast<float>(WSLDB_NUMERIC_LITERAL(*this) / WSLDB_NUMERIC_LITERAL(d)));
    }

    Datum operator/=(const Datum& d) {
        *this = *this / d;
        return *this;
    }

    bool operator==(const Datum& d) {
        switch (type_) {
            case DatumType::Int8:
            case DatumType::Float4:
                return WSLDB_NUMERIC_LITERAL(*this) - WSLDB_NUMERIC_LITERAL(d) == 0;
            case DatumType::Bool:
                return AsBool() - d.AsBool() == 0;
            case DatumType::Text:
                return AsString().compare(d.AsString()) == 0;
            default:
                //std::cout << "invalid type for comparing data: " << TokenTypeToString(type_) << std::endl;
                break;
        }        
        return false;
    }

    bool operator!=(const Datum& d) {
        return !(*this == d);
    }

    bool operator<(const Datum& d) {
        switch (type_) {
            case DatumType::Int8:
            case DatumType::Float4:
                return WSLDB_NUMERIC_LITERAL(*this) - WSLDB_NUMERIC_LITERAL(d) < 0;
            case DatumType::Bool:
                return AsBool() - d.AsBool() < 0;
            case DatumType::Text:
                return AsString().compare(d.AsString()) < 0;
            default:
                //std::cout << "invalid type for comparing data: " << TokenTypeToString(type_) << std::endl;
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

public:
    static std::string SerializeData(const std::vector<Datum>& data) {
        std::string value;
        for (Datum d: data) {
            value += d.Serialize();
        }
        return value;
    }

static std::string TypeToString(DatumType type) {
    switch (type) {
        case DatumType::Int8:
            return "int8";
        case DatumType::Float4:
            return "float4";
        case DatumType::Text:
            return "text";
        case DatumType::Bool:
            return "bool";
        case DatumType::Null:
            return "null";
        case DatumType::Bytea:
            return "bytea";
        default:
            return "invalid datum type";
    }
}

static bool TypeIsNumeric(DatumType type) {
    return type == DatumType::Int8 ||
           type == DatumType::Float4;
}


static bool TypeIsInteger(DatumType type) {
    return type == DatumType::Int8;
}

private:
    DatumType type_;
    std::string data_;
};

}

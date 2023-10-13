#include "datum.h"

namespace wsldb {

std::string DatumTypeToString(DatumType type) {
    switch (type) {
        case DatumType::Int4:
            return "int4";
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
        default:
            return "invalid datum type";
    }
}

bool DatumTypeIsNumeric(DatumType type) {
    return type == DatumType::Int4 ||
           type == DatumType::Int8 ||
           type == DatumType::Float4;
}


bool DatumTypeIsInteger(DatumType type) {
    return type == DatumType::Int4 ||
           type == DatumType::Int8;
}

}

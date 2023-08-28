#pragma once

#include "expr.h"

namespace wsldb {

class Tuple {
public:
    Tuple(std::vector<Expr*> exprs): exprs(std::move(exprs)) {}
    std::vector<Expr*> exprs;
};

}

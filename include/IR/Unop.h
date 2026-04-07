#pragma once

#include <llvm/IR/Value.h>

#include "Defs.h"

namespace p2cllvm{

enum class UnOp { NOT = 0, EXTRACT_YEAR };
class Builder;

struct trivial_unop{
    ValueRef<> createUnOp(Builder& builder, UnOp op, ValueRef<> val);
};
 
};


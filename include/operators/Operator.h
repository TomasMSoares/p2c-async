#pragma once

#include <functional>

#include "Iu.h"
#include "IR/Builder.h"

namespace p2cllvm {
using ConsumerFn = std::function<void(Builder&)>;
using InitFn = std::function<void(Builder&)>;

class Operator {
    public:
        virtual void produce(IUSet &required, Builder &builder, ConsumerFn consumer, InitFn fn) = 0;
        virtual IUSet availableIUs() = 0;
        virtual ~Operator() = default;

};
} // namespace p2cllvm

#pragma once
#include <unordered_set>
#include <string>

#include <fmt/format.h>
#include "json.hpp"
#include "Expression.hpp"

class Operator {
protected:
    static int operatorCount;
    std::string operatorName;
    
public:
    std::unordered_set<IU> providedIus; 
    virtual ~Operator() = default;
    virtual std::string compile(std::unordered_set<IU>) = 0;
};
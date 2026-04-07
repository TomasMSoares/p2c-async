#pragma once
#include "Operator.hpp"

class Map final : public Operator {
    std::unique_ptr<Operator> input;
    std::vector<Expression> expressions;

public:
    Map(const nlohmann::json& op, std::unique_ptr<Operator> i) : input(std::move(i)){
        providedIus = input->providedIus;
        for (const auto& val : op["values"]) {
            IU iu(val["iu"]["iu"].get<std::string>(), val["iu"]["type"]["type"].get<std::string>());
            expressions.emplace_back(val["exp"], iu.name);
            providedIus.insert(iu);
            IU::knownTypes[iu.name] = iu.type;
        }
    }

    std::string compile(std::unordered_set<IU> requestedIUs){
        operatorName = input->compile(requestedIUs);
        for (const auto& expr : expressions){
            expr.compile(operatorName);
        }
        return operatorName;
    }
};
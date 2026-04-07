#pragma once
#include "Operator.hpp"

class Select final : public Operator {
    std::unique_ptr<Operator> input;
    Expression condition;

public:
    Select(const nlohmann::json& op, std::unique_ptr<Operator> i) : input(std::move(i)), condition(op["condition"]){
        providedIus = input->providedIus;
        operatorName = fmt::format("sel{}", operatorCount++);
    }

    std::string compile(std::unordered_set<IU> requestedIus) override {
        std::string inputName = input->compile(requestedIus);
        std::string formattedCondition = condition.compile();
        fmt::print("auto {} = make_unique<Selection>(std::move({}), std::move({}));\n", operatorName, inputName, formattedCondition);
        return operatorName;
    }

};
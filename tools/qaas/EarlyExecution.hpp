#pragma once
#include "Operator.hpp"

struct Mapping {
    IU before;
    IU after;

    Mapping(std::string b, std::string a) : before(b), after(a) {}
};

class EarlyExecution final : public Operator {
    std::unique_ptr<Operator> input;
    std::vector<Mapping> mappings;

public:
    EarlyExecution(const nlohmann::json op, std::unique_ptr<Operator> i) : input(std::move(i)){
        for (const auto& mapping : op["mapping"]["mapping"]){
            Mapping newMapping(mapping["key"], mapping["value"]);
            mappings.push_back(newMapping);
            providedIus.insert(newMapping.after);
            IU::knownTypes[newMapping.after.name] = newMapping.before.type;
        }
    }

    std::string compile(std::unordered_set<IU> requestedIus) override {
        operatorName = input->compile(requestedIus);
        for (const auto& mapping : mappings){
            fmt::print("IU* {} = {};\n", mapping.after.name, mapping.before.name);
        }
        return operatorName;
    }


};
#pragma once
#include "Operator.hpp"

struct Key {
    IU iu;
    Expression input;
};

enum AggType {
    COUNT_STAR,
    AVG,
    SUM,
    MIN,
    MAX,
    ANY
};

struct Aggregate {
    AggType type;
    IU iu;
    std::optional<Expression> input;

    Aggregate(AggType t, IU i, std::optional<Expression> e) : type(t), iu(i), input(e){}
};

class GroupBy final : public Operator {
    std::unique_ptr<Operator> input;

    std::vector<IU> valueIus;
    std::vector<IU> avgIus;
    std::optional<IU> countIu = std::nullopt;

    std::vector<Key> keys;
    std::vector<Aggregate> aggregates;

public:
    GroupBy(const nlohmann::json& op, std::unique_ptr<Operator> i) : input(std::move(i)) {
        operatorName = fmt::format("gb{}", operatorCount++);
        const nlohmann::json& values = op["values"];

        for (const auto& key : op["key"]) {
            IU iu(key["iu"]["iu"].get<std::string>(), key["iu"]["type"]["type"].get<std::string>());
            providedIus.insert(iu);
            const auto& value = values[key["arg"].get<int>()];
            Expression expr(value, iu.name);
            keys.emplace_back(iu, expr);
        }

        for (const auto& agg : op["aggregates"]) {
            AggType type;
            IU iu(agg["iu"]["iu"].get<std::string>(), agg["iu"]["type"]["type"].get<std::string>());
            providedIus.insert(iu);
            std::optional<Expression> expr = std::nullopt;
            if (agg.contains("arg")){
                int arg = agg["arg"];
                expr = Expression(values[arg], iu.name + "input");
            } 
         
            std::string t = agg["op"];
            if (t == "countstar") {
                countIu = iu;
                type = AggType::COUNT_STAR;
            } else if (t == "avg") {
                avgIus.push_back(iu);
                type = AggType::AVG;
            } else if (t == "sum") {
                type = AggType::SUM;
            } else if (t == "min") {
                type = AggType::MIN;
            }else if (t == "max") {
                type = AggType::MAX;
            }else if (t == "any") {
                type = AggType::ANY;
            } else {
                throw std::runtime_error{"aggregation function " + t + " is not implemented"};
            }
            aggregates.emplace_back(type, iu, expr);
        }
    }

    std::string compile(std::unordered_set<IU> requestedIUs) {
        std::string inputName = input->compile(requestedIUs);

        std::vector<std::string> groupKeys;
        for (const auto& key : keys){
            groupKeys.push_back(key.input.compile(inputName));
        }

        for (const auto& agg : aggregates){
            if (agg.input){
                agg.input.value().compile(inputName);
            }
        }

        if (groupKeys.empty()){
            fmt::print("auto {} = make_unique<Aggregation>(std::move({}), IUSet());\n", operatorName, inputName);
        }else{
            fmt::print("auto {} = make_unique<Aggregation>(std::move({}), IUSet({{{}}}));\n", operatorName,
                inputName, fmt::join(groupKeys, ", "));
        }   

        for (const auto& agg : aggregates) {
            std::string iuName = agg.iu.name;
            std::optional<std::string> inputIu = std::nullopt;
            switch (agg.type){
                case AggType::COUNT_STAR:
                    fmt::print("{}->addAggregate(make_unique<CountAggregate>(\"{}\"));\n", operatorName, iuName);
                    break;
                case AggType::AVG:
                    inputIu = iuName + "input";
                    iuName += "prepare";
                    fmt::print("{}->addAggregate(make_unique<SumAggregate>(\"{}\", {}));\n", operatorName, iuName, inputIu.value());
                    break;
                case AggType::SUM:
                    inputIu = iuName + "input";
                    fmt::print("{}->addAggregate(make_unique<SumAggregate>(\"{}\", {}));\n", operatorName, iuName, inputIu.value());
                    break;
                case AggType::MIN:
                    inputIu = iuName + "input";
                    fmt::print("{}->addAggregate(make_unique<MinAggregate>(\"{}\", {}));\n", operatorName,
                            iuName, iuName + "input");
                    break;
                case AggType::MAX:
                    inputIu = iuName + "input";
                    fmt::print("{}->addAggregate(make_unique<MaxAggregate>(\"{}\", {}));\n", operatorName, iuName, iuName + "input");
                    break;
                case AggType::ANY:
                    inputIu = iuName + "input";
                    fmt::print("{}->addAggregate(make_unique<AnyAggregate>(\"{}\", {}));\n", operatorName, iuName, iuName + "input");
                    break;
            }
            assert(agg.iu.type !=p2c::Type::Undefined);
            IU iu(iuName, agg.iu.type);
            if (inputIu){
                IU iuInput(inputIu.value(), agg.iu.type);
            }
            fmt::print("IU* {0} = {1}->getIU(\"{0}\");\n", iu.name, operatorName);
        }

        // compute actual values for averages
        if (!avgIus.empty()) {
            if (!countIu.has_value()) {
                countIu = IU(operatorName + "_count", "integer");
                fmt::print("{}->addAggregate(make_unique<CountAggregate>(\"{}\"));\n", operatorName,
                        countIu.value().name);
                fmt::print("IU* {0} = {1}->getIU(\"{0}\");\n", countIu.value().name, operatorName);
            }

            nlohmann::json baseExpr = {{"expression", "div"},
                                    {"left", {{"expression", "iuref"}, {"iu", "placeholder"}}},
                                    {"right", {{"expression", "iuref"}, {"iu", countIu.value().name}}}};
            for (auto iu : avgIus) {
                baseExpr["left"]["iu"] = iu.name + "prepare";
                Expression expr(baseExpr, iu.name);
                expr.compile(operatorName);
            }
        }
        return operatorName;
    }
};

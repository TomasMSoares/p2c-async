#pragma once
#include "Operator.hpp"

class Join final : public Operator {
    std::unique_ptr<Operator> left, right;
    std::vector<std::string> leftJoinConds, rightJoinConds;
    std::vector<Expression> nonEquiJoinConds;
    std::string joinType;

    void extractSingleIU(const nlohmann::json& expr) {
        assert(expr["expression"] == "iuref");
        
        const IU iu(expr["iu"]);
        if (left->providedIus.contains(iu)){
            leftJoinConds.push_back(iu.name);
        } else if (right->providedIus.contains(iu)){
            rightJoinConds.push_back(iu.name);
        } else {
            std::cout << "left available ius: ";
            for (const auto& iu : left->providedIus) std::cout << iu.name << " ";
            std ::cout << std::endl << "right available ius: ";
            for (const auto& iu : right->providedIus) std::cout << iu.name << " ";
            std::cout << std::endl;
            throw std::runtime_error{"Unknown iu in join condition " + iu.name};
        }
    }

    void extractSingleCondition(const nlohmann::json& condition){
        if (condition["expression"] == "compare" && condition["direction"] == "="){
            extractSingleIU(condition["left"]);
            extractSingleIU(condition["right"]);
        }else{
            nonEquiJoinConds.emplace_back(condition);
        }
    }
    
    void extractConditions(const nlohmann::json& expr) {
        const std::string& type = expr["expression"];
        if (type == "and") {
            for (const auto& condition : expr["input"]) {
                extractSingleCondition(condition);
            }
            return;
        } else if (type == "compare") {
            return extractSingleCondition(expr);
        } 
        throw std::runtime_error{"join condition of " + type + " is not implemented!"};
    }
    
public:
    Join(const nlohmann::json& op, std::unique_ptr<Operator> l, std::unique_ptr<Operator> r) : left(std::move(l)), right(std::move(r)){
        operatorName = fmt::format("join{}", operatorCount++);
        extractConditions(op["condition"]);
        providedIus.insert(left->providedIus.begin(), left->providedIus.end());
        providedIus.insert(right->providedIus.begin(), right->providedIus.end());

        std::string umbraJoinType = op["type"];
        if (umbraJoinType == "inner"){
            joinType = "InnerJoin";
        }else if (umbraJoinType == "leftsemi"){
            joinType = "LeftSemiJoin";
        }else if (umbraJoinType == "leftouter"){
            joinType = "LeftOuterJoin";
        }else if (umbraJoinType == "leftanti"){
            joinType = "LeftAntiJoin";
        }else if (umbraJoinType == "rightanti"){
            joinType = "RightAntiJoin";
        }else{
            throw std::runtime_error{"Unknown umbra join type " + umbraJoinType};
        }
    }

    std::string compile(std::unordered_set<IU> requestedIus) override {
        std::string formattedNonEquiConds = "nullptr";
        bool first = true;
        for (const auto& cond : nonEquiJoinConds){
            std::string condition = cond.compile();
            if (first){
                formattedNonEquiConds = condition;
                first = false;
            }else{
                formattedNonEquiConds = fmt::format("makeCallExp(\"logical_and()\", {}, {})", condition, formattedNonEquiConds);
            }
        }
        fmt::print(
            "auto {} = make_unique<{}>(std::move({}), std::move({}), std::vector<IU*>{{{}}}, std::vector<IU*>{{{}}}, {});\n",
            operatorName, joinType, left->compile(requestedIus), right->compile(requestedIus), 
            fmt::join(leftJoinConds, ", "),
            fmt::join(rightJoinConds, ", "),
            formattedNonEquiConds
        );
        return operatorName;
    }
};
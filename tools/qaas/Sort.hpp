#pragma once
#include "Operator.hpp"

class Sort final : public Operator {
    struct SortKey {
        IU iu;
        bool descending;
    };

    std::unique_ptr<Operator> input;
    std::vector<SortKey> sortKeys;

    std::optional<int> limit = std::nullopt;

public:
    Sort(const nlohmann::json& op, std::unique_ptr<Operator>&& i) : input(std::move(i)){
        operatorName = fmt::format("sort{}", operatorCount++);

        if (op.contains("limit")){
            limit = op["limit"];
        }

        for (const auto& sortKey : op["order"]) {
            assert(sortKey["value"]["expression"] == "iuref");
            IU iu(sortKey["value"]["iu"]);
            sortKeys.emplace_back(iu, sortKey.contains("descending"));
        }
    }

    std::string compile(std::unordered_set<IU> requestedIUs) override {
        std::vector<std::string> sortIus, descending;

        for (const auto& key : sortKeys){
            sortIus.push_back(key.iu.name);
            descending.push_back(key.descending ? "true" : "false");
        }

        if (limit.has_value()){
            fmt::print("auto {} = make_unique<TopK>(std::move({}), vector<IU*>{{{}}}, std::vector<bool>{{{}}}, {});\n", operatorName, input->compile(requestedIUs), fmt::join(sortIus, ", "), fmt::join(descending, ", "), limit.value());
        }else{
            fmt::print("auto {} = make_unique<Sort>(std::move({}), vector<IU*>{{{}}}, std::vector<bool>{{{}}});\n", operatorName, input->compile(requestedIUs), fmt::join(sortIus, ", "), fmt::join(descending, ", "));
        }
        
        return operatorName;
    }
};
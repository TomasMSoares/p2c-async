#include "Operator.hpp"

class TableScan final : public Operator {
    static int selCount;
    static size_t cardinality;
    
    std::string tableName;
    std::vector<Expression> restrictions;
    std::unordered_map<IU, std::string> columnName;

    void addRestriction(Expression restr, std::string& inputName){
        std::string iuName = restr.compile(inputName);
        fmt::print("auto selIu{} = make_unique<IUExp>({});\n", selCount++, iuName);
        fmt::print("auto sel{0} = make_unique<Selection>(std::move({1}), make_unique<IUExp>({2}));\n", operatorCount, inputName, iuName);
        inputName = fmt::format("sel{}", operatorCount++);
    }
public:
    static std::string largestTable;
    static bool USE_PQLITE;
    static bool VECTORIZED_EXPRESSIONS;

    TableScan(const nlohmann::json& op) {
        tableName = op["tablename"];
        operatorName = fmt::format("{}{}", tableName, operatorCount++);
        for (const auto& attr : op["attributes"]) {
            IU iu(attr["iu"]["iu"].get<std::string>(), attr["iu"]["type"]["type"].get<std::string>());
            providedIus.insert(iu);
            columnName[iu] = attr["name"];
        }

        if (op.contains("restrictions")) {
            for (const auto& cond : op["restrictions"]){
                const std::string& mode = cond["mode"];
                if (mode == "expression"){
                    restrictions.emplace_back(cond["value"]);
                    continue;
                }
                nlohmann::json restriction = {{"expression", "compare"}};
                restriction["direction"] = cond["mode"];
                int attribute = cond["attribute"];
                restriction["left"] = op["attributes"][attribute]["iu"];
                restriction["left"]["expression"] = "iuref";
                restriction["right"] = cond["value"];
                if (mode == "[]"){
                    restriction["direction"] = ">=";
                }
            
                restrictions.emplace_back(restriction);
                if (mode == "[]"){
                    restriction["direction"] = "<=";
                    restriction["right"] = cond["upper"];
                    restrictions.emplace_back(restriction);
                }
            }
        }

        if (op.contains("residuals")) {
            for (const auto& res : op["residuals"]){
                restrictions.emplace_back(res);
            }
        }

        // determine largest table
        if (op["cardinality"] > cardinality){
            cardinality = op["cardinality"];
            largestTable = tableName;
        }
    }

    std::string compile(std::unordered_set<IU> requestedIUs) override {
        std::string op = USE_PQLITE ? "VectorScan" : "Scan";
        fmt::print("auto {} = make_unique<{}>(\"{}\");\n", operatorName, op, tableName);

        for (const auto& iu : providedIus){
             fmt::print("IU* {} = {}->getIU(\"{}\");\n", iu.name, operatorName, columnName[iu]);
        }

        std::string inputName = operatorName;
        if (VECTORIZED_EXPRESSIONS){
            std::vector<Expression> leftoverExpressions;
            for (auto& restr : restrictions){
                if (auto vectorized = restr.compileVectorized()){
                    fmt::print("{}->addPredicate({});", operatorName, vectorized.value());
                }else{
                    leftoverExpressions.push_back(restr);
                }

                for (const auto& restr : leftoverExpressions){
                    addRestriction(restr, inputName);
                }
            }
        }else{
            for (const auto& restr : restrictions){
                addRestriction(restr, inputName);
            }
        }
        return inputName;
    };
};
#pragma once
#include <string>
#include <iostream>
#include <fmt/ranges.h>

#include "json.hpp"
#include "IU.hpp"
#include "../include/provided/types.hpp"

struct Expr {
        std::string formatted;
        p2c::Type type;

        Expr(std::string f, p2c::Type t) : formatted(f), type(t){}

        Expr(std::string f, std::string umbraType) : formatted(f){
            type = parseType(umbraType);
        }

        std::string getCppType() const {
            if (type == p2c::Type::Date) return "int32_t";
            return p2c::tname(type);
        }

        std::string getP2cType() const{
            switch (type){
                case p2c::Type::Integer:
                    return "TypeEnum::Integer";
                case p2c::Type::Double:
                    return "TypeEnum::Double";
                case p2c::Type::Char:
                    return "TypeEnum::Char";
                case p2c::Type::String:
                    return "TypeEnum::String";
                case p2c::Type::BigInt:
                    return "TypeEnum::BigInt";
                case p2c::Type::Bool:
                    return "TypeEnum::Bool";
                case p2c::Type::Date:
                    return "TypeEnum::Date";
            }
            throw std::runtime_error{"unkown p2c type"};
        }
    };

class Expression {
    static int exprCount;
    nlohmann::json expr;
    IU iu;

    Expr extractConstValue(const nlohmann::json& value) const {
        const std::string& dataType = value["type"]["type"];
        if (dataType == "numeric" || dataType == "bignumeric") {
            double baseValue = value["value"];
            int scale = value["type"].contains("scale") ? value["type"]["scale"].get<int>() : 0;
            return {std::to_string(baseValue / pow(10, scale)), dataType};
        }else if (dataType == "date" || dataType == "integer") {
            return {std::to_string(value["value"].get<int>()), dataType};
        }else if (dataType == "char" || dataType == "text"){
            return {"\"" + value["value"].get<std::string>() + "\"", dataType};
        }else if (dataType == "char1"){
            return {"\'" + std::string{static_cast<char>(value["value"].get<int>())} +"\'", dataType};
        }
        throw std::runtime_error("data type " + dataType + " is not implemented!");
    }

    Expr extractConst(const nlohmann::json& constVal) const  {
        assert(constVal["expression"] == "const");
        return extractConstValue(constVal["value"]);
    }

    Expr translateCase(const nlohmann::json& expr) const {
        bool isSimple = expr["expression"] == "simplecase";
        std::vector<std::string> cases;
        std::vector<std::string> results;
    
        Expr defaultValue = extractConst(expr["else"]);

        nlohmann::json simpleAdapter{{"expression", "compare"}, {"direction", "="}};
        if (isSimple){
            simpleAdapter["left"] =expr["input"];
            for (const auto& c : expr["cases"]){
                simpleAdapter["right"] = c["value"];
                cases.push_back(translateExpression(simpleAdapter).formatted);
                results.push_back(translateExpression(c["result"]).formatted);
            }
        }else{
            for (const auto& c : expr["cases"]){
                cases.push_back(translateExpression(c["condition"]).formatted);
                results.push_back(translateExpression(c["result"]).formatted);
            }
        }
        
        return {fmt::format("make_unique<CaseExp<{0}>>( array<unique_ptr<Exp>, {0}>{{{1}}}, array<unique_ptr<Exp>, {0}>{{{2}}}, make_unique<ConstExp<{3}>>({4}), {5})", 
            cases.size(), 
            fmt::join(cases, ", "), 
            fmt::join(results, ", "), 
            defaultValue.getCppType(),
            defaultValue.formatted, 
            defaultValue.getP2cType()), 
        defaultValue.type};
    }

    Expr translateExpression(const nlohmann::json& expr) const {
        const std::string& type = expr["expression"];
        std::string function;
        p2c::Type t = p2c::Type::Undefined;
        if (type == "iuref") {
            IU iu(expr["iu"]);
            p2c::Type t = IU::knownTypes.contains(iu.name) ? IU::knownTypes[iu.name] : p2c::Type::Undefined;
            return {fmt::format("make_unique<IUExp>({})", iu.name), t};
        } else if (type == "const") {
            Expr constExpr = extractConst(expr);
            return {fmt::format("make_unique<ConstExp<{}>>({})", constExpr.getCppType(), constExpr.formatted), constExpr.type};
        } else if (type == "cast") {
            return {translateExpression(expr["input"]).formatted, expr["type"]["type"].get<std::string>()};
        } else if (type == "mul") {
            function = "std::multiplies()";
        } else if (type == "div") {
            function = "std::divides()";
        } else if (type == "add") {
            function = "std::plus()";
        } else if (type == "sub") {
            function = "std::minus()";
        } else if (type == "compare"){
            const std::string& dir = expr["direction"];
            if (dir == "<=") {
                function = "std::less_equal()";
            } else if (dir == "<") {
                function = "std::less()";
            } else if (dir == ">=") {
                function = "std::greater_equal()";
            } else if (dir == ">") {
                function = "std::greater()";
            } else if (dir == "="){
                function = "std::equal_to()";
            } else if (dir == "<>") {
                function = "std::not_equal_to()";
            } else {
                std::cout << expr << std::endl;
                throw std::runtime_error("direction " + dir + " is not supported!");
            }
            t = p2c::Type::Bool;
        } else if (type == "simplecase") {
            return translateCase(expr);
        } else if (type == "like"){
            const auto& inputs = expr["input"];
            Expr input = translateExpression(inputs[0]);
            std::string pattern = inputs[1]["value"]["value"];
            return {fmt::format("make_unique<LikeExp>({}, \"{}\")", input.formatted, pattern), p2c::Type::Bool};
        }else if (type == "contains"){
            Expr input = translateExpression(expr["input"][0]);
            std::string contained = expr["input"][1]["value"]["value"];
            return {fmt::format("make_unique<LikeExp>({}, \"%{}%\")", input.formatted, contained), p2c::Type::Bool};
        }else if (type == "startswith"){
            Expr input = translateExpression(expr["input"][0]);
            std::string contained = expr["input"][1]["value"]["value"];
            return {fmt::format("make_unique<LikeExp>({}, \"{}%\")", input.formatted, contained), p2c::Type::Bool};
        } else if (type == "searchedcase") {
           return translateCase(expr);
        }else if (type == "extractyear") {
            return {fmt::format("makeCallExp(\"date::extractYear\", {})", translateExpression(expr["input"]).formatted), p2c::Type::Integer};
        }else if (type == "in"){
            std::vector<std::string> inputs;
            for (const auto& input : expr["input"]){
                inputs.push_back(translateExpression(input).formatted);
            }

            std::vector<Expr> constants;
            for (const auto& constValue : expr["values"]){
                constants.push_back(extractConstValue(constValue));
            }

            int i = 0;
            std::string result, option;
            bool first = true;
            while (i < constants.size()){
                bool firstInput = true;
                for (const auto& input : inputs){
                    const auto& constVal = constants[i++];
                    std::string s = fmt::format("makeCallExp(\"equal_to()\", {}, make_unique<ConstExp<{}>>({}))", input, constVal.getCppType(), constVal.formatted);
                    if (firstInput){
                        firstInput = false;
                        option = s;
                    }else{
                        option = fmt::format("makeCallExp(\"logical_and()\", {}, {})", option, s);
                    }
                }
                if (first){
                    first = false;
                    result = option;
                }else{
                    result = fmt::format("makeCallExp(\"logical_or()\", {}, {})", result, option);
                }
            }
            return {result, p2c::Type::Bool};
        } else if (type == "not"){
            Expr input = translateExpression(expr["input"]);
            return {fmt::format("makeCallExp(\"logical_not()\", {})", input.formatted), p2c::Type::Bool};
        } else if (type == "or"){
            std::string result;
            bool first = true;
            for (const auto& clause : expr["input"]){
                std::string formatted = translateExpression(clause).formatted;
                if (first){
                    first = false;
                    result = formatted;
                }else{
                    result = fmt::format("makeCallExp(\"logical_or()\", {}, {})", result, formatted);
                }
            }
            return {result, p2c::Type::Bool};
        } else if (type == "and"){
            std::string result;
            bool first = true;
            for (const auto& clause : expr["input"]){
                std::string formatted = translateExpression(clause).formatted;
                if (first){
                    first = false;
                    result = formatted;
                }else{
                    result = fmt::format("makeCallExp(\"logical_and()\", {}, {})", result, formatted);
                }
            }
            return {result, p2c::Type::Bool};
        }else if (type == "between"){
            Expr input = translateExpression(expr["input"][0]);
            Expr lowerBound = translateExpression(expr["input"][1]);
            Expr upperBound = translateExpression(expr["input"][2]);
            return {fmt::format("makeCallExp(\"logical_and()\", makeCallExp(\"greater_equal()\", {0}, {1}), makeCallExp(\"less_equal()\", {0}, {2}))", input.formatted, lowerBound.formatted, upperBound.formatted), p2c::Type::Bool};
        } else {
            std::cout << expr << std::endl;
            throw std::runtime_error{"expression " + type + " is not supported!"};
        }
        Expr leftExpr = translateExpression(expr["left"]);
        Expr rightExpr = translateExpression(expr["right"]);
        if (t == p2c::Type::Undefined) t = leftExpr.type;
        if (t == p2c::Type::Undefined) t = rightExpr.type;
        return {fmt::format("makeCallExp(\"{}\", {}, {})", function, 
           leftExpr.formatted, rightExpr.formatted), t};
    }

    std::string translateRootExpression(std::string& input) const {
        std::string exprName = fmt::format("expr{}", exprCount);
        std::string operatorName = fmt::format("map{}", exprCount++);
        const std::string& type = expr["expression"];
        if (type == "iuref") {
            IU oldIu(expr["iu"]);
            fmt::print("IU* {} = {};\n", iu.name, oldIu.name);
            return iu.name;
        }
        Expr expression = translateExpression(expr);
        fmt::print("auto {} = {};\n", exprName, expression.formatted);
        fmt::print("auto {} = make_unique<Map>(std::move({}), std::move({}), \"{}\", {});\n",
                operatorName, input, exprName, iu.name, expression.getP2cType());
        fmt::print("IU* {0} = {1}->getIU(\"{0}\");\n", iu.name, operatorName);
        input = operatorName;
        return iu.name;
    }

    std::string initIu(std::string desiredIuName){
        if (desiredIuName.size() != 0) {
            return desiredIuName;
        }
        return fmt::format("computedIU{}", exprCount++);
    }

    std::string translateTablePredicateOperand(nlohmann::json& expr){
        const std::string& type = expr["expression"];
        if (type == "iuref"){
            IU iu(expr["iu"]);
            return fmt::format("TablePredicateOperand::column({})", iu.name);
        }else if (type == "const"){
            Expr constVal = extractConstValue(expr["value"]);
            return fmt::format("TablePredicateOperand::constant<{}>({})", tname(constVal.type) , constVal.formatted);
        }else{
            throw std::runtime_error("Wrong predicate " + type + " for VectorizedExpression");
        }
    }
public:
    Expression(const nlohmann::json& json, std::string iuName = "") : expr(json), iu(initIu(iuName)){}

    std::string compile(std::string& input) const {
        return translateRootExpression(input);
    }

    std::string compile() const {
        return translateExpression(expr).formatted;
    }

    std::optional<std::string> compileVectorized() {
        const std::string& type = expr["expression"];
        if (type == "compare"){
            const std::string& kind = expr["direction"];
            std::string predKind = "pred::Kind::";
            if (kind == "="){
                predKind += "Equal";
            }else if (kind == "<>"){
                predKind += "NotEqual";
            }else if (kind == "<"){
                predKind += "LessThan";
            }else if (kind == "<="){
                predKind += "LessThanEqual";
            }else if (kind == ">"){
                predKind += "GreaterThan";
            }else if (kind == ">="){
                predKind += "GreaterThanEqual";
            }else{
                throw std::runtime_error("Unknown direction " + kind);
            }
            return fmt::format("make_unique<TablePredicateExp>({}, {}, {})", predKind, 
                translateTablePredicateOperand(expr["left"]), translateTablePredicateOperand(expr["right"]));
        }else{
            return std::nullopt;
        }
    }
};

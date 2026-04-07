#pragma once
#include <string>
#include <unordered_map>
#include "../include/provided/types.hpp"

p2c::Type parseType(std::string type){
    if (type == "bignumeric" || type.starts_with("numeric")){
        return p2c::Type::Double;
    }else if (type == "date"){
        return p2c::Type::Date;
    }else if (type == "integer"){
        return p2c::Type::Integer;
    }else if (type == "bigint"){
        return p2c::Type::BigInt;
    }else if (type == "char1" || type == "char(1)"){
        return p2c::Type::Char;
    }else if (type == "char" || type == "text" || type.starts_with("varchar")){
        return p2c::Type::String;
    }else if (type.starts_with("char")){
        return p2c::Type::String;
    }else {
        throw std::runtime_error{"Unknown umbra type " + type};
    }
}

struct IU {
    static std::unordered_map<std::string, p2c::Type> knownTypes;
    std::string name;
    p2c::Type type = p2c::Type::Undefined;

    IU(std::string s) : name(s){
        std::string charsToReplace = "#()*";
        for (auto& c : name) {
            if (charsToReplace.find(c) != std::string::npos) {
                c = '_';
            }
        }
        if (knownTypes.find(name) != knownTypes.end()){
            type = knownTypes[name];
        }
    }

    IU(std::string s, p2c::Type t) : IU(s){
        type = t;
        knownTypes[name] = t;
    }

    IU(std::string s, std::string t) : IU(s, parseType(t)){}

    bool operator==(const IU& iu) const {
        return iu.name == name;
    }
};

namespace std {
    template <>
    struct hash<IU> {
        size_t operator()(const IU& iu) const {
            return std::hash<std::string>()(iu.name);
        }
    };
}

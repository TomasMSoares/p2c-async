#pragma once
#include <unordered_map>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <array>


#include "json.hpp"
#include "IU.hpp"

#include <fmt/core.h>

class UmbraWebInterfaceConnector {

    static std::ostringstream getFromWebinterface(std::string sqlQuery, std::string endpoint, std::string jq){
        std::replace(sqlQuery.begin(), sqlQuery.end(), '\n', ' ');
        // construct command to read from umbra to check the result

        static std::string curl = R"(curl -s -X POST -H "Content-Type: text/plain" --data)";
        
        // Command to execute curl and jq
        std::string command = fmt::format(
            R"({} "set search_path = tpchSf1, public; {}" "https://umbra.db.in.tum.de/api/{}" | {})", curl, sqlQuery, endpoint, jq);
        // Open a pipe to read the command output
        std::ostringstream outputStream;
        {
            std::array<char, 128> buffer;
            FILE* pipe = popen(command.c_str(), "r");
            if (!pipe) throw std::runtime_error("Failed to run command.");
            while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
                outputStream << buffer.data();
            }
            int returnCode = pclose(pipe);
            if (returnCode != 0) {
                throw std::runtime_error("Command execution failed.");
            }
        }
        return outputStream;
    }

    static std::ostringstream getResultFromWebinterface(std::string sqlQuery){
        static std::string jq = "jq .";
        return getFromWebinterface(sqlQuery, "query", jq);
    }

    static std::ostringstream getPlanFromWebinterface(std::string sqlQuery){
        return getFromWebinterface(sqlQuery, "planVerbose", "jq .");
    }

    static std::string rtrim(std::string s, const char* t = " \t\n\r\f\v"){
        s.erase(s.find_last_not_of(t) + 1);
        return s;
    }

public:
    static std::unordered_map<IU, std::vector<std::string>> getResult(std::string sqlQuery) {
        std::ostringstream outputStream = getResultFromWebinterface(sqlQuery);
        auto json = nlohmann::json::parse(outputStream.str());
        // construct the result
        std::unordered_map<IU, std::vector<std::string>> map;
        auto result = json["results"][0];

        for (size_t i=0; i!=result["columns"].size(); i++){
            const auto& column = result["columns"][i];
            const auto& values = result["result"][i];
            std::string type = column["type"].get<std::string>();
            IU iu(column["name"].get<std::string>(), type);
            std::vector<std::string> vals;
            for (const auto& val : values){
                if (val.is_string()){
                    if (type.starts_with("char(")){
                        vals.push_back(rtrim(val));
                    }else{
                        vals.push_back(val);
                    }
                }else if (val.is_number()){
                    vals.push_back(std::to_string(val.get<long>()));
                }
            }
            map[iu] = vals; 
        }
        return map;
    }

    static std::string getPlan(std::string sqlQuery){
        return getPlanFromWebinterface(sqlQuery).str();
    }
};

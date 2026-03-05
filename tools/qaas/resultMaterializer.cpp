#include "UmbraWebinterfaceConnector.hpp"
#include <string>
#include <iostream>
#include <fstream>

#include <fmt/ranges.h>

std::string loadFileToString(const std::string& filename) {
  std::ifstream file(filename);
  if (!file) {
      throw std::runtime_error("Failed to open file: " + filename);
  }
  std::ostringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}

std::unordered_map<std::string, p2c::Type> IU::knownTypes = {};

int main(int argc, char** argv){
    if (argc != 2){
      std::cout << "Usage ./result_fetcher.out <folder-location>";
    }
    std::string folder = argv[1];
    std::string query = loadFileToString(folder + "/definition.sql");
    auto result = UmbraWebInterfaceConnector::getResult(query);
    const std::string expectedVarPrefix = "expected";

    std::vector<std::string> mapValues;
    for (auto& el : result) {
       const auto iu = el.first;
       const auto& vals = el.second;
       fmt::print("std::vector<{}> {}_{} = {{", "string", expectedVarPrefix, iu.name);
        for (size_t i = 0; i < vals.size(); i++) {
          fmt::print("\"{}\"", vals[i]); 
          if (i + 1 < vals.size()) fmt::print(", ");
        }
        fmt::print("}};\n");

        mapValues.push_back(fmt::format("{{\"{0}\", {1}_{0}}}", iu.name, expectedVarPrefix));
    }

    fmt::print("std::unordered_map<std::string, std::vector<std::string>> expectedValues{{{}}};\n", fmt::join(mapValues, ","));
     
}


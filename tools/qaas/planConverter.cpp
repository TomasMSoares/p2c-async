#include "EarlyExecution.hpp"
#include "GroupBy.hpp"
#include "Join.hpp"
#include "Map.hpp"
#include "Operator.hpp"
#include "Select.hpp"
#include "Sort.hpp"
#include "TableScan.hpp"

#include "UmbraWebinterfaceConnector.hpp"

#include <fmt/core.h>
#include <fmt/format.h>
#include <fstream>
#include <iostream>
#include <string>

#include "json.hpp"

// Working Q01 - Q10, Q12, Q14, Q17 - Q21
// TODO PG Q11 singletonjoin
// TODO PG Q13 Non star count, left outer join
// TODO PG Q15 pipelinebreakerscan, singletonjoin
// TODO PG Q16 count distinct, rightantijoin (untested)

int Expression::exprCount = 0;
int Operator::operatorCount = 0;
int TableScan::selCount = 0;
bool TableScan::USE_PQLITE = false;
bool TableScan::VECTORIZED_EXPRESSIONS = false;
std::string TableScan::largestTable = "";
size_t TableScan::cardinality;
std::unordered_map<std::string, p2c::Type> IU::knownTypes = {};

std::string loadFileToString(const std::string &filename) {
  std::ifstream file(filename);
  if (!file) {
    throw std::runtime_error("Failed to open file: " + filename);
  }
  std::ostringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}

std::unique_ptr<Operator> translateOperator(const nlohmann::json &op) {
  const std::string &type = op["operator"];
  if (type == "join") {
    return std::make_unique<Join>(op, translateOperator(op["left"]),
                                  translateOperator(op["right"]));
  } else if (type == "groupby") {
    return std::make_unique<GroupBy>(op, translateOperator(op["input"]));
  } else if (type == "sort") {
    return std::make_unique<Sort>(op, translateOperator(op["input"]));
  } else if (type == "map") {
    return std::make_unique<Map>(op, translateOperator(op["input"]));
  } else if (type == "tablescan") {
    return std::make_unique<TableScan>(op);
  } else if (type == "earlyexecution") {
    return std::make_unique<EarlyExecution>(op, translateOperator(op["input"]));
  } else if (type == "select") {
    return std::make_unique<Select>(op, translateOperator(op["input"]));
  }
  throw std::runtime_error{"operator " + type + " is not implemented!"};
}

int main(int argc, char **argv) {
  assert(argc == 2 || argc == 3);

  std::string folder = "queries/definitions/";
  folder += argv[1];
  std::string query = loadFileToString(folder + "/definition.sql");

  int mode = 0;
  if (argc == 3) {
    mode = atoi(argv[2]);
  }
  TableScan::USE_PQLITE = mode > 0;
  TableScan::VECTORIZED_EXPRESSIONS = mode > 1;

  auto map = UmbraWebInterfaceConnector::getPlan(query);
  auto j = nlohmann::json::parse(map);
  const auto &step = j["optimizersteps"][6]["plan"];
  auto rootOperator = translateOperator(step["plan"]);
  fmt::print("#include \"internal.h\"\n");
  fmt::print("#include \"result.hpp\"\n");

  fmt::print("int main(){{\n");
  fmt::print("p2cllvm::initialize();\n");

  std::unordered_set<IU> outputIus;
  std::string rootName = rootOperator->compile(outputIus);

  std::vector<std::string> formattedOutputIus;
  std::vector<std::string> names;
  for (const auto &outputValue : step["output"]) {
    names.push_back(fmt::format("\"{}\"", std::string(outputValue["name"])));
    // For output iurefs, directly use the IU name (no alias needed to avoid redeclaration)
    if (outputValue["iu"]["expression"] == "iuref") {
      IU iu(outputValue["iu"]["iu"].get<std::string>());
      formattedOutputIus.push_back(iu.name);
    } else {
      Expression expr(outputValue["iu"], outputValue["name"]);
      formattedOutputIus.push_back(expr.compile(rootName));
    }
  }

  // COMMENT OUT DEPENDING ON ASSERT OR PRINT
  fmt::print("produce(std::move({}), std::vector<IU*>{{{}}}, "
             "std::vector<std::string>{{{}}}, make_unique<AssertTupleSink>(expectedValues));\n",
             rootName, fmt::join(formattedOutputIus, ", "),
             fmt::join(names, ","));

  // fmt::print("produce(std::move({}), std::vector<IU*>{{{}}}, "
  //            "std::vector<std::string>{{{}}}, make_unique<PrintTupleSink>());\n",
  //            rootName, fmt::join(formattedOutputIus, ", "),
  //            fmt::join(names, ","));

  fmt::print("return 0;\n}}\n");
  return 0;
}

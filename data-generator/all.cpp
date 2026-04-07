#include "csv.hpp"
#include "table-reader.hpp"
#include "tpch.hpp"

#include <iostream>
#include <array>
#include <filesystem>
#include <string_view>

using namespace p2c;
namespace fs = std::filesystem;

void output_columnfiles(const std::string &outdir, const fs::path &iprefix);

int main(int argc, char *argv[]) {
  // takes two arguments: the directory containing all database tables
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <input-dir> <mode:[columnfiles|pqlite]> <output='output'>" << std::endl;
    return 1;
  }
  bool pqlite = std::string_view(argv[2]) == "pqlite";
  std::string out = argc > 3 ? argv[3] : "output";
  fs::path iprefix(argv[1]);
  if (pqlite) {
    throw std::runtime_error{"Will be added later in the semester!"};
  } else {
    output_columnfiles(out, iprefix);
  }
  return 0;
}

void output_columnfiles(const std::string &outdir, const fs::path &iprefix) {
  // orders
  {
    orders::reader reader(outdir + "/orders/", (iprefix / "orders.tbl").c_str(), orders_c.data());
    auto rows = reader.read();
    std::cout << "read " << rows << " rows for orders" << std::endl;
  }
  // nation
  {
    nation::reader reader(outdir + "/nation/", (iprefix / "nation.tbl").c_str(), nation_c.data());
    auto rows = reader.read();
    std::cout << "read " << rows << " rows for nation" << std::endl;
  }
  // customer
  {
    customer::reader reader(outdir + "/customer/", (iprefix / "customer.tbl").c_str(), customer_c.data());
    auto rows = reader.read();
    std::cout << "read " << rows << " rows for customer" << std::endl;
  }
  // lineitem
  {
    lineitem::reader reader(outdir + "/lineitem/", (iprefix / "lineitem.tbl").c_str(), lineitem_c.data());
    auto rows = reader.read();
    std::cout << "read " << rows << " rows for lineitem" << std::endl;
  }
  // part
  {
    part::reader reader(outdir + "/part/", (iprefix / "part.tbl").c_str(), part_c.data());
    auto rows = reader.read();
    std::cout << "read " << rows << " rows for part" << std::endl;
  }
  // partsupp
  {
    partsupp::reader reader(outdir + "/partsupp/", (iprefix / "partsupp.tbl").c_str(), partsupp_c.data());
    auto rows = reader.read();
    std::cout << "read " << rows << " rows for partsupp" << std::endl;
  }
  // region
  {
    region::reader reader(outdir + "/region/", (iprefix / "region.tbl").c_str(), region_c.data());
    auto rows = reader.read();
    std::cout << "read " << rows << " rows for region" << std::endl;
  }
  // supplier
  {
    supplier::reader reader(outdir + "/supplier/", (iprefix / "supplier.tbl").c_str(), supplier_c.data());
    auto rows = reader.read();
    std::cout << "read " << rows << " rows for supplier" << std::endl;
  }
}

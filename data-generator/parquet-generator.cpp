#include "tpch.hpp"
#include "csv.hpp"
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <memory>
#include <string>

using namespace p2c;
namespace fs = std::filesystem;
using comp = parquet::Compression;

template<typename T>
std::shared_ptr<arrow::DataType> getArrowType() {
    if constexpr (std::is_same_v<T, int32_t>) return arrow::int32();
    else if constexpr (std::is_same_v<T, int64_t>) return arrow::int64();
    else if constexpr (std::is_same_v<T, double>) return arrow::float64();
    else if constexpr (std::is_same_v<T, char>) return arrow::uint8();
    else if constexpr (std::is_same_v<T, date>) return arrow::date32();
    else if constexpr (std::is_same_v<T, std::string_view>) return arrow::utf8();
    else throw std::runtime_error("Unsupported type");
}

template<typename T>
std::string getSchemaType() {
    if constexpr (std::is_same_v<T, int32_t>) return "INTEGER";
    else if constexpr (std::is_same_v<T, int64_t>) return "BIGINT";
    else if constexpr (std::is_same_v<T, double>) return "DOUBLE";
    else if constexpr (std::is_same_v<T, char>) return "CHAR";
    else if constexpr (std::is_same_v<T, date>) return "DATE";
    else if constexpr (std::is_same_v<T, std::string_view>) return "STRING";
    else throw std::runtime_error("Unsupported type");
}

template<typename T>
arrow::Status buildArrayFromPtr(const T* data, int64_t count, std::shared_ptr<arrow::Array>* out) {
    if constexpr (std::is_same_v<T, int32_t>) {
        arrow::Int32Builder builder; ARROW_RETURN_NOT_OK(builder.AppendValues(data, count)); return builder.Finish(out);
    } else if constexpr (std::is_same_v<T, int64_t>) {
        arrow::Int64Builder builder; ARROW_RETURN_NOT_OK(builder.AppendValues(data, count)); return builder.Finish(out);
    } else if constexpr (std::is_same_v<T, double>) {
        arrow::DoubleBuilder builder; ARROW_RETURN_NOT_OK(builder.AppendValues(data, count)); return builder.Finish(out);
    } else if constexpr (std::is_same_v<T, char>) {
        arrow::UInt8Builder builder;
        for (int64_t i = 0; i < count; ++i) ARROW_RETURN_NOT_OK(builder.Append(static_cast<uint8_t>(data[i])));
        return builder.Finish(out);
    } else if constexpr (std::is_same_v<T, date>) {
        arrow::Date32Builder builder;
        for (int64_t i = 0; i < count; ++i) ARROW_RETURN_NOT_OK(builder.Append(data[i].value));
        return builder.Finish(out);
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        arrow::StringBuilder builder;
        for (int64_t i = 0; i < count; ++i) ARROW_RETURN_NOT_OK(builder.Append(data[i].data(), data[i].size()));
        return builder.Finish(out);
    } else {
        return arrow::Status::NotImplemented("Unsupported type");
    }
}

template<typename... Ts>
class ParquetTableWriter {
public:
    using TableDef = p2c::TableDef<Ts...>;
    using Import = typename TableDef::import;

    ParquetTableWriter(const std::string& table_name, const std::array<const char*, sizeof...(Ts)>& column_names)
        : table_name_(table_name)
        , column_names_(column_names)
        , importer_(nullptr) {}

    void process(const fs::path& csv_path, const std::string& local_output_dir, int start_file_idx, int64_t rows_per_file, comp::type compression_type) {
        if (!fs::exists(csv_path)) {
            std::cerr << "Skipping " << table_name_ << ": " << csv_path << " not found." << std::endl;
            return;
        }
        
        // Ensure table-specific directory exists
        fs::path table_dir = fs::path(local_output_dir) / table_name_;
        fs::create_directories(table_dir);

        importer_ = std::make_unique<Import>(csv_path.c_str());
        importer_->read();
        int64_t total_rows = importer_->row_count();
        if (total_rows == 0) return;

        nlohmann::json meta_json;
        fs::path local_meta_path = table_dir / (table_name_ + "_metadata.json");
        
        if (fs::exists(local_meta_path)) {
            std::ifstream meta_in(local_meta_path);
            meta_in >> meta_json;
        } else {
            meta_json["table_name"] = table_name_;
            meta_json["row_groups"] = nlohmann::json::array();
        }

        std::vector<std::shared_ptr<arrow::Field>> fields;
        size_t idx = 0;
        buildSchema<0>(fields, idx);
        auto schema = std::make_shared<arrow::Schema>(fields);

        int64_t rows_written = 0;
        int current_file_idx = start_file_idx;

        while (rows_written < total_rows) {
            int64_t chunk_size = std::min(rows_per_file, total_rows - rows_written);
            
            std::string filename = table_name_ + "_" + std::to_string(current_file_idx) + ".parquet";
            fs::path local_file_path = table_dir / filename;

            auto out_result = arrow::io::FileOutputStream::Open(local_file_path.string());
            if(!out_result.ok()) throw std::runtime_error("Open failed: " + out_result.status().ToString());

            std::shared_ptr<arrow::io::FileOutputStream> outfile = *out_result;
            auto props = parquet::WriterProperties::Builder().compression(compression_type)->build();
            auto arrow_props = parquet::ArrowWriterProperties::Builder().build();
            
            auto result = parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), outfile, props, arrow_props);
            if (!result.ok()) throw std::runtime_error("Writer creation failed: " + result.status().ToString());
            
            std::unique_ptr<parquet::arrow::FileWriter> writer = std::move(*result);

            std::vector<std::shared_ptr<arrow::Array>> arrays;
            buildArraysNoCopy<0>(arrays, rows_written, chunk_size);
            auto table = arrow::Table::Make(schema, arrays, chunk_size);
            
            if (!writer->WriteTable(*table, chunk_size).ok()) throw std::runtime_error("WriteTable failed");
            
            writer->Close();
            outfile->Close();

            auto file_md = writer->metadata();
            size_t total_uncompressed = 0;
            for (int i = 0; i < file_md->num_columns(); ++i) {
                total_uncompressed += file_md->RowGroup(0)->ColumnChunk(i)->total_uncompressed_size();
            }

            nlohmann::json rg;
            rg["id"] = current_file_idx;
            rg["object_key"] = filename;
            rg["compressed_size"] = fs::file_size(local_file_path);
            rg["uncompressed_size"] = total_uncompressed;
            rg["num_rows"] = chunk_size;
            meta_json["row_groups"].push_back(rg);

            std::cout << "    Wrote " << filename << " (" << chunk_size << " rows) to " << table_name_ << " directory" << std::endl;

            rows_written += chunk_size;
            current_file_idx++;
        }

        std::ofstream meta_out(local_meta_path);
        meta_out << meta_json.dump(2);
    }

    static void generateSchemaJson(const std::string& output_path) {
        nlohmann::json schema_json;
        schema_json["tables"] = nlohmann::json::array();
        addTableSchema<nation>(schema_json, "nation", nation_c);
        addTableSchema<customer>(schema_json, "customer", customer_c);
        addTableSchema<lineitem>(schema_json, "lineitem", lineitem_c);
        addTableSchema<orders>(schema_json, "orders", orders_c);
        addTableSchema<part>(schema_json, "part", part_c);
        addTableSchema<partsupp>(schema_json, "partsupp", partsupp_c);
        addTableSchema<region>(schema_json, "region", region_c);
        addTableSchema<supplier>(schema_json, "supplier", supplier_c);
        std::ofstream out(output_path);
        out << schema_json.dump(2);
    }

private:
    template<size_t I>
    void buildSchema(std::vector<std::shared_ptr<arrow::Field>>& fields, size_t& idx) {
        if constexpr (I < sizeof...(Ts)) {
            using T = std::tuple_element_t<I, std::tuple<Ts...>>;
            fields.push_back(arrow::field(column_names_[I], getArrowType<T>()));
            idx++;
            buildSchema<I + 1>(fields, idx);
        }
    }

    template<size_t I>
    void buildArraysNoCopy(std::vector<std::shared_ptr<arrow::Array>>& arrays, int64_t offset, int64_t count) {
        if constexpr (I < sizeof...(Ts)) {
            using T = std::tuple_element_t<I, std::tuple<Ts...>>;
            auto& column = std::get<I>(importer_->outputs);
            std::shared_ptr<arrow::Array> array;
            auto status = buildArrayFromPtr(column.items.data() + offset, count, &array);
            if (!status.ok()) throw std::runtime_error("Failed to build array: " + status.ToString());
            arrays.push_back(array);
            buildArraysNoCopy<I + 1>(arrays, offset, count);
        }
    }

    template<typename TDef, size_t N>
    static void addTableSchema(nlohmann::json& schema_json, const std::string& table_name, const std::array<const char*, N>& col_names) {
        nlohmann::json table_json;
        table_json["name"] = table_name;
        table_json["columns"] = nlohmann::json::array();
        addColumns<TDef, 0>(table_json["columns"], col_names);
        schema_json["tables"].push_back(table_json);
    }
    
    template<typename TDef, size_t I>
    static void addColumns(nlohmann::json& columns_json, const auto& col_names) {
        if constexpr (I < std::tuple_size_v<typename TDef::columns>) {
            using T = std::tuple_element_t<I, typename TDef::columns>;
            nlohmann::json col_json;
            col_json["name"] = col_names[I];
            col_json["type"] = getSchemaType<T>();
            columns_json.push_back(col_json);
            addColumns<TDef, I + 1>(columns_json, col_names);
        }
    }

    std::string table_name_;
    std::array<const char*, sizeof...(Ts)> column_names_;
    std::unique_ptr<Import> importer_;
};

parquet::Compression::type GetCompressionType(const std::string& type) {
    if (type == "snappy") return parquet::Compression::SNAPPY;
    if (type == "zstd")   return parquet::Compression::ZSTD;
    if (type == "gzip")   return parquet::Compression::GZIP;
    if (type == "lz4")    return parquet::Compression::LZ4;
    return parquet::Compression::UNCOMPRESSED;
}

int main(int argc, char* argv[]) {
    // Expected arguments: <in-tbl-path> <local-out-dir> <table-name> <start-idx> <rows-per-file> [comp]
    if (argc < 6) { 
        std::cerr << "Usage: " << argv[0] << " <in-tbl-path> <local-out-dir> <table-name> <start-idx> <rows-per-file> [comp]" << std::endl;
        return 1; 
    }
    
    fs::path input_tbl(argv[1]);
    std::string local_out_dir(argv[2]);
    std::string table = argv[3];
    int start_idx = std::stoi(argv[4]);
    int64_t rows_per_file = std::stoll(argv[5]);
    auto comp_type = (argc > 6) ? GetCompressionType(argv[6]) : parquet::Compression::UNCOMPRESSED;

    try {
        fs::create_directories(local_out_dir);

        if (table == "schema") {
            ParquetTableWriter<int32_t>::generateSchemaJson(local_out_dir + "/tpch_schema.json");
            return 0;
        }

        if (table == "nation") {
            ParquetTableWriter<int32_t, std::string_view, int32_t, std::string_view> w("nation", nation_c); 
            w.process(input_tbl, local_out_dir, start_idx, rows_per_file, comp_type);
        }
        else if (table == "customer") {
            ParquetTableWriter<int32_t, std::string_view, std::string_view, int32_t, std::string_view, double, std::string_view, std::string_view> w("customer", customer_c); 
            w.process(input_tbl, local_out_dir, start_idx, rows_per_file, comp_type);
        }
        else if (table == "lineitem") {
            ParquetTableWriter<int64_t, int32_t, int32_t, int32_t, double, double, double, double, char, char, date, date, date, std::string_view, std::string_view, std::string_view> w("lineitem", lineitem_c); 
            w.process(input_tbl, local_out_dir, start_idx, rows_per_file, comp_type);
        }
        else if (table == "orders") {
            ParquetTableWriter<int64_t, int32_t, char, double, date, std::string_view, std::string_view, int32_t, std::string_view> w("orders", orders_c); 
            w.process(input_tbl, local_out_dir, start_idx, rows_per_file, comp_type);
        }
        else if (table == "part") {
            ParquetTableWriter<int32_t, std::string_view, std::string_view, std::string_view, std::string_view, int32_t, std::string_view, double, std::string_view> w("part", part_c); 
            w.process(input_tbl, local_out_dir, start_idx, rows_per_file, comp_type);
        }
        else if (table == "partsupp") {
            ParquetTableWriter<int32_t, int32_t, int32_t, double, std::string_view> w("partsupp", partsupp_c); 
            w.process(input_tbl, local_out_dir, start_idx, rows_per_file, comp_type);
        }
        else if (table == "region") {
            ParquetTableWriter<int32_t, std::string_view, std::string_view> w("region", region_c); 
            w.process(input_tbl, local_out_dir, start_idx, rows_per_file, comp_type);
        }
        else if (table == "supplier") {
            ParquetTableWriter<int32_t, std::string_view, std::string_view, int32_t, std::string_view, double, std::string_view> w("supplier", supplier_c); 
            w.process(input_tbl, local_out_dir, start_idx, rows_per_file, comp_type);
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
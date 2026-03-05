#pragma once
#include <filesystem>
#include <functional>
#include <unordered_map>
#include "csv.hpp"
#include "pqlite.hpp"
#include "compression.hpp"
#include "debug.hpp"
#include "io.hpp"

namespace p2c {

template <typename T>
struct PQLiteColumnChunkBuffer {
  using value_t = T;
  using page_t = DataColumn<T>;
  using stats_t = pqlite::PQLiteStatistics<T>;

  stats_t stats;
  std::vector<T> items;

  explicit PQLiteColumnChunkBuffer(unsigned expected_rows = 1024)
      : items() {
    items.reserve(expected_rows);
  }

  ~PQLiteColumnChunkBuffer() {
    assert(items.empty() && "Buffer not flushed");
  }

  void append(const T &val) {
    items.push_back(val);
    stats.track(val);
  }

  [[nodiscard]] unsigned buffered_rows() const {
    return items.size();
  }

  void clear() {
    items.clear();
    stats.clear();
  }
};

template <typename... Ts>
class PQLiteTableReader {
    static constexpr char delim = '|';

  FileMapping<char> input;
  pqlite::PQLFileWriter writer;
  std::array<std::string_view, sizeof...(Ts)> colnames;
  std::tuple<PQLiteColumnChunkBuffer<Ts>...> buffers;
  unsigned rowgroup_size;

  std::array<unsigned, pqlite::pql_encoding_count> used_encodings;

 public:
  PQLiteTableReader(const std::string &output_file, const char *filename, std::span<const char *const, sizeof...(Ts)> colnames,
unsigned rowgroup_size = 64 * 1024)
      : input(filename)
      , writer(output_file.c_str())
      , rowgroup_size(rowgroup_size) {
    for (size_t i = 0; i < sizeof...(Ts); i++) {
      this->colnames[i] = colnames[i];
    }
    std::fill(used_encodings.begin(), used_encodings.end(), 0);
  }

  size_t read() {
    std::vector<unsigned> columns(sizeof...(Ts));
    for (auto i = 0u; i != sizeof...(Ts); ++i) {
      columns[i] = i;
    }

    unsigned rows = csv::read_file<delim>(input, columns, [&](unsigned col, csv::CharIter &pos) {
      fold_buffers(0, [&]<unsigned idx>(auto &output, unsigned num, unsigned v) {
        if (idx == col) {
          using value_t =
              typename std::remove_reference<decltype(output)>::type::value_t;
          csv::Parser<value_t> parser;
          auto value = parser.template parse_value<delim>(pos);
          output.append(value);
        }
        return 0;
      });
      if (col + 1 == sizeof...(Ts)) {
        flush_buffers(false);
      }
    });

    flush_buffers(true);
    return rows;
  }

  ~PQLiteTableReader() { writer.finalizeFooter(colnames); }

 private:
  template <typename T, typename F, unsigned I = 0>
  constexpr inline T fold_buffers(T init_value, const F &fn) {
    if constexpr (I == sizeof...(Ts)) {
      return init_value;
    } else {
      return fold_buffers<T, F, I + 1>(fn.template operator()<I>(std::get<I>(buffers), sizeof...(Ts), init_value), fn);
    }
  }

  void flush_buffers(bool force) {
    auto rows = std::get<0>(buffers).buffered_rows();
    if (!force && rows < rowgroup_size) {
      return;
    }
    std::array<pqlite::ColumnChunkWriter, sizeof...(Ts)> chunks;
    size_t est_used_bytes = 0;
    fold_buffers(0, [&, this]<unsigned I>(auto &output, unsigned num, unsigned v) {
      using value_t = typename std::decay<decltype(output)>::type::value_t;
      est_used_bytes += output.stats.output_size;
      auto idx = I;
      ensure(std::get<I>(buffers).buffered_rows() == rows, "Buffered rows mismatch");
      chunks[I] = pqlite::ColumnChunkWriter([this, idx](pqlite::PQLColumnChunk &target) {
        auto& out = std::get<I>(this->buffers);
        auto est_output = out.stats.output_size;
        auto [bytes, encoding] =
            pqlite::PQLColumnCompression<value_t>::compress(target.data, {out.items}, out.stats);
        this->used_encodings[static_cast<unsigned>(encoding)]++;
        target = {.size_bytes = bytes, .encoding = encoding, .type = p2c::type_tag<value_t>::tag};
        if (est_output < bytes) {
          std::cout << "  wrote " << bytes << " bytes in " << to_string(encoding) << std::endl
                    << "     vs " << est_output << " uncompressed"
                    << " in col " << this->colnames[I] << " (" << p2c::tname(p2c::type_tag<value_t>::tag) << ")" << std::endl;
        }
        out.clear();
        assert(out.stats.output_size == out.stats.initialOutputSize());
      });
      return 0;
    });
    /// write to output
    writer.addRowGroup(chunks, rows, static_cast<size_t>(static_cast<double>(est_used_bytes) * 1.2));
  }

};  // struct TableReader

} // namespace p2c

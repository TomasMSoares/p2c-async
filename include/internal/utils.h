#pragma once
#include <string>
#include <string_view>
#include <stdexcept>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <tuple>

namespace p2cllvm {

// Standalone helper function for local mem mapping
inline std::pair<void*, size_t> map_local_file(std::string_view dirPath, std::string_view colName) {
  std::string filePath = std::string(dirPath) + "/" + std::string(colName) + ".bin";
  int fd = open(filePath.c_str(), O_RDONLY);
  if (fd == -1)
    throw std::runtime_error("Failed to open file: " + filePath);

  // Get file size
  off_t fsize = lseek(fd, 0, SEEK_END);
  lseek(fd, 0, SEEK_SET);
  size_t size = static_cast<size_t>(fsize);

  auto *data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
  if (data == MAP_FAILED) {
      auto err = errno;
      close(fd);
      throw std::logic_error("Could not map file: " + std::string(strerror(err)));
  }
  close(fd);
  
  // Optimization for large files
  if (size > 1024 * 1024) {
    madvise(data, size, MADV_HUGEPAGE);
  }
  return {data, size};
}

// Helper to detect if path is remote (contains ://)
inline bool isRemotePath(std::string_view path) {
    return path.find("://") != std::string_view::npos;
}

} // namespace p2cllvm
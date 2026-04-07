#pragma once

#include <cstdint>
#include <iostream>
#include <string_view>


namespace p2cllvm {

/// magic constants for murmurhash
constexpr inline uint64_t MURMURHASH_MAGIC64A = 0xc6a4a7935bd1e995;
constexpr inline uint64_t MURMURHASH_SEED = 0x8445d61a4e774912;

// Helper for safe unaligned loads
// prevents page boundary segfaults
inline uint64_t load64(const void* p) {
  uint64_t v;
  std::memcpy(&v, p, sizeof(uint64_t));
  return v;
}

/// generic murmurhash function for variable-size keys
constexpr uint64_t murmurHash(const char* key, uint32_t len, uint64_t seed =MURMURHASH_SEED);

/// simplified version of the variable-size version
constexpr uint64_t hash64bit(uint64_t k) {
  // MurmurHash64A
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;
  uint64_t h = 0x8445d61a4e774912 ^ (8 * m);
  k *= m;
  k ^= k >> r;
  k *= m;
  h ^= k;
  h *= m;
  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

constexpr uint64_t hashString(std::string_view str) {
    return murmurHash(str.data(), str.size());
}

/***
 * Adapted from https://github.com/ksss/digest-murmurhash/blob/master/ext/digest/murmurhash/64a.c
 *
 * Copyright (c) 2013 ksss
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * */
constexpr uint64_t murmurHash(const char* key, uint32_t len, uint64_t seed) {
  using u64 = uint64_t;
  const u64 m = MURMURHASH_MAGIC64A;
  const int r = 47;

  u64 h = seed ^ (len * m);

  // Replaced std::bit_cast logic with char pointer arithmetic
  const char* data = key;
  const char* end = data + (len & ~7); // Stop at the last full 8-byte chunk

  while (data != end) {
    u64 k = load64(data); // Safe unaligned load
    data += 8;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const auto* data2 = std::bit_cast<const unsigned char*>(data);

  switch (len & 7) {
    case 7:
      h ^= (static_cast<u64>(data2[6]) << 48); [[fallthrough]];
    case 6:
      h ^= (static_cast<u64>(data2[5]) << 40); [[fallthrough]];
    case 5:
      h ^= (static_cast<u64>(data2[4]) << 32); [[fallthrough]];
    case 4:
      h ^= (static_cast<u64>(data2[3]) << 24); [[fallthrough]];
    case 3:
      h ^= (static_cast<u64>(data2[2]) << 16); [[fallthrough]];
    case 2:
      h ^= (static_cast<u64>(data2[1]) << 8);  [[fallthrough]];
    case 1:
      h ^= (static_cast<u64>(data2[0]));
      h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}
}

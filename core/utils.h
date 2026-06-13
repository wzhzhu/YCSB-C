//
//  utils.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/5/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_UTILS_H_
#define YCSB_C_UTILS_H_

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <random>

namespace utils {

const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325;
const uint64_t kFNVPrime64 = 1099511628211;

inline uint64_t FNVHash64(uint64_t val) {
  uint64_t hash = kFNVOffsetBasis64;

  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hash = hash ^ octet;
    hash = hash * kFNVPrime64;
  }
  return hash;
}

inline uint64_t Hash(uint64_t val) { return FNVHash64(val); }

// Base seed for the per-thread RNG streams. Deterministic by default (0); set
// the YCSB_RNG_SEED env var to vary the trace reproducibly across experiments.
inline uint64_t RngSeedBase() {
  static const uint64_t base = [] {
    const char *e = std::getenv("YCSB_RNG_SEED");
    return e != nullptr ? std::strtoull(e, nullptr, 10) : 0ULL;
  }();
  return base;
}

// Thread-local engine: the previous shared static engine was a data race (UB)
// under multi-threaded clients, making traces non-deterministic across runs and
// schemes. Each thread now gets a distinct, deterministically-derived seed, so
// the aggregate access multiset is identical across runs/schemes for a given
// thread count -> reproducible hit ratios.
inline std::default_random_engine &ThreadLocalRng() {
  static std::atomic<uint64_t> stream_counter{0};
  static thread_local std::default_random_engine generator(
      static_cast<std::default_random_engine::result_type>(
          RngSeedBase() + 0x9E3779B97F4A7C15ULL *
                              (stream_counter.fetch_add(
                                   1, std::memory_order_relaxed) +
                               1)));
  return generator;
}

inline double RandomDouble(double min = 0.0, double max = 1.0) {
  std::uniform_real_distribution<double> uniform(min, max);
  return uniform(ThreadLocalRng());
}

///
/// Returns an ASCII code that can be printed to desplay
///
inline char RandomPrintChar() {
  return rand() % 94 + 33;
}

class Exception : public std::exception {
 public:
  Exception(const std::string &message) : message_(message) { }
  const char* what() const noexcept {
    return message_.c_str();
  }
 private:
  std::string message_;
};

inline bool StrToBool(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
  if (str == "true" || str == "1") {
    return true;
  } else if (str == "false" || str == "0") {
    return false;
  } else {
    throw Exception("Invalid bool string: " + str);
  }
}

inline std::string Trim(const std::string &str) {
  auto front = std::find_if_not(str.begin(), str.end(), [](int c){ return std::isspace(c); });
  return std::string(front, std::find_if_not(str.rbegin(), std::string::const_reverse_iterator(front),
      [](int c){ return std::isspace(c); }).base());
}

} // utils

#endif // YCSB_C_UTILS_H_

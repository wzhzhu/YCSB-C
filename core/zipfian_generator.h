//
//  zipfian_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/7/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_ZIPFIAN_GENERATOR_H_
#define YCSB_C_ZIPFIAN_GENERATOR_H_

#include <cassert>
#include <cmath>
#include <cstdint>
#include <atomic>
#include <mutex>
#include "utils.h"

namespace ycsbc {

class ZipfianGenerator : public Generator<uint64_t> {
 public:
  constexpr static const double kZipfianConst = 0.99;
  static const uint64_t kMaxNumItems = (UINT64_MAX >> 24);
  
  ZipfianGenerator(uint64_t min, uint64_t max,
                   double zipfian_const = kZipfianConst) :
      num_items_(max - min + 1), base_(min), theta_(zipfian_const),
      zeta_n_(0), n_for_zeta_(0) {
    assert(num_items_ >= 2 && num_items_ < kMaxNumItems);
    zeta_2_ = Zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    RaiseZeta(num_items_);
    eta_ = Eta();
    
    Next();
  }
  
  ZipfianGenerator(uint64_t num_items) :
      ZipfianGenerator(0, num_items - 1, kZipfianConst) { }
  
  uint64_t Next(uint64_t num_items);
  
  uint64_t Next() { return Next(num_items_); }

  uint64_t Last();
  
 private:
  ///
  /// Compute the zeta constant needed for the distribution.
  /// Remember the number of items, so if it is changed, we can recompute zeta.
  ///
  void RaiseZeta(uint64_t num) {
    assert(num >= n_for_zeta_);
    zeta_n_ = Zeta(n_for_zeta_, num, theta_, zeta_n_);
    n_for_zeta_ = num;
  }
  
  double Eta() {
    return (1 - std::pow(2.0 / num_items_, 1 - theta_)) /
        (1 - zeta_2_ / zeta_n_);
  }

  ///
  /// Calculate the zeta constant needed for a distribution.
  /// Do this incrementally from the last_num of items to the cur_num.
  /// Use the zipfian constant as theta. Remember the new number of items
  /// so that, if it is changed, we can recompute zeta.
  ///
  static double Zeta(uint64_t last_num, uint64_t cur_num,
                     double theta, double last_zeta) {
    double zeta = last_zeta;
    for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
      zeta += 1 / std::pow(i, theta);
    }
    return zeta;
  }
  
  static double Zeta(uint64_t num, double theta) {
    return Zeta(0, num, theta, 0);
  }
  
  uint64_t num_items_;
  uint64_t base_; /// Min number of items to generate
  
  // Computed parameters for generating the distribution
  double theta_, zeta_n_, eta_, alpha_, zeta_2_;
  uint64_t n_for_zeta_; /// Number of items used to compute zeta_n
  std::atomic<uint64_t> last_value_;
  std::mutex mutex_;
};

// Hot path is lock-free. The per-thread randomness already comes from a
// thread_local RNG (utils::ThreadLocalRng), so the original global mutex only
// serialised reads of the distribution parameters plus the last_value_ write.
// Those parameters (zeta_n_, eta_, alpha_, theta_, base_, num_items_) are
// written exactly once during single-threaded construction (RaiseZeta / Eta);
// for a fixed-size YCSB workload num never exceeds n_for_zeta_ afterwards, so
// no thread ever re-enters the write path concurrently. Holding one std::mutex
// on every key draw instead funnelled all worker threads through a single
// critical section that also ran two std::pow calls, which at high thread
// counts (t128) degenerated into a futex lock-convoy: ~59% of CPU was burned in
// native_queued_spin_lock_slowpath under ScrambledZipfianGenerator::Next,
// producing metastable throughput collapses that were mis-attributed to the
// cache. The rare grow path keeps a double-checked lock for correctness.
inline uint64_t ZipfianGenerator::Next(uint64_t num) {
  assert(num >= 2 && num < kMaxNumItems);

  if (num > n_for_zeta_) { // Recompute zeta_n and eta (grow; not hit by YCSB)
    std::lock_guard<std::mutex> lock(mutex_);
    if (num > n_for_zeta_) {
      RaiseZeta(num);
      eta_ = Eta();
    }
  }

  double u = utils::RandomDouble();
  double uz = u * zeta_n_;

  uint64_t value;
  if (uz < 1.0) {
    value = 0;
  } else if (uz < 1.0 + std::pow(0.5, theta_)) {
    value = 1;
  } else {
    value = base_ + num * std::pow(eta_ * u - eta_ + 1, alpha_);
  }
  last_value_.store(value, std::memory_order_relaxed);
  return value;
}

inline uint64_t ZipfianGenerator::Last() {
  return last_value_.load(std::memory_order_relaxed);
}

}

#endif // YCSB_C_ZIPFIAN_GENERATOR_H_

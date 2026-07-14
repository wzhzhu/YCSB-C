#include "db/rocksdb_db.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "cache/multi_level_cache.h"
#include "cache/multi_level_cache_allocator.h"
#include "cache/sr_hyper_clock_cache.h"
#include "cache/arc_cache.h"
#include "cache/cacheus_cache.h"
#include "cache/sharded_wrapper_cache.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

namespace ycsbc {

namespace {

const char* kRocksdbDirKey = "rocksdb.dir";
const char* kDefaultRocksdbDir = "/tmp/ycsbc-rocksdb";
const char* kRocksdbSyncWriteKey = "rocksdb.sync_write";
const char* kRocksdbCacheTypeKey = "rocksdb.cache_type";

bool ParseBool(const utils::Properties& props, const std::string& key,
               bool default_value) {
  std::string value = props.GetProperty(key, default_value ? "true" : "false");
  std::transform(value.begin(), value.end(), value.begin(), ::tolower);
  return value == "1" || value == "true" || value == "yes" || value == "on";
}

int ParseInt(const utils::Properties& props, const std::string& key,
             int default_value) {
  std::string value = props.GetProperty(key, std::to_string(default_value));
  try {
    return std::stoi(value);
  } catch (...) {
    return default_value;
  }
}

uint64_t ParseUint64(const utils::Properties& props, const std::string& key,
                     uint64_t default_value) {
  std::string value = props.GetProperty(key, std::to_string(default_value));
  try {
    return static_cast<uint64_t>(std::stoull(value));
  } catch (...) {
    return default_value;
  }
}

double ParseDouble(const utils::Properties& props, const std::string& key,
                   double default_value) {
  std::string value = props.GetProperty(key, std::to_string(default_value));
  try {
    return std::stod(value);
  } catch (...) {
    return default_value;
  }
}

bool TryParseDouble(const std::string& text, double* out) {
  if (out == nullptr) {
    return false;
  }
  try {
    size_t consumed = 0;
    const double parsed = std::stod(text, &consumed);
    while (consumed < text.size() &&
           std::isspace(static_cast<unsigned char>(text[consumed]))) {
      ++consumed;
    }
    if (consumed != text.size()) {
      return false;
    }
    *out = parsed;
    return true;
  } catch (...) {
    return false;
  }
}

std::string SanitizeMetricKey(std::string key) {
  for (char& ch : key) {
    if (!(std::isalnum(static_cast<unsigned char>(ch)) || ch == '_')) {
      ch = '_';
    }
  }
  return key;
}

void EmitNumericCacheOptionsAsMetrics(const std::shared_ptr<rocksdb::Cache>& cache) {
  if (cache == nullptr) {
    return;
  }
  const std::string options = cache->GetPrintableOptions();
  std::istringstream iss(options);
  std::string line;
  while (std::getline(iss, line)) {
    if (line.empty()) {
      continue;
    }
    const size_t sep = line.find('=');
    if (sep == std::string::npos || sep == 0 || sep + 1 >= line.size()) {
      continue;
    }
    const std::string key = SanitizeMetricKey(line.substr(0, sep));
    const std::string value_str = line.substr(sep + 1);
    double value = 0.0;
    if (!TryParseDouble(value_str, &value)) {
      continue;
    }
    std::cerr << "rocksdb\t" << key << "\t" << value << std::endl;
  }
}

bool EndsWith(const std::string& value, const std::string& suffix) {
  if (value.size() < suffix.size()) {
    return false;
  }
  return value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

rocksdb::CompressionType ParseCompressionType(const std::string& name) {
  std::string lower = name;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
  if (lower == "none" || lower == "no" || lower == "k nocompression") {
    return rocksdb::kNoCompression;
  }
  if (lower == "snappy") {
    return rocksdb::kSnappyCompression;
  }
  if (lower == "lz4") {
    return rocksdb::kLZ4Compression;
  }
  if (lower == "zstd") {
    return rocksdb::kZSTD;
  }
  return rocksdb::kNoCompression;
}

std::shared_ptr<rocksdb::Cache> CreateMultiLevelCache(
    const utils::Properties& props, size_t cache_capacity,
    std::shared_ptr<rocksdb::MultiLevelCache>* out_multi_level_cache) {
  const std::string cache_type =
      props.GetProperty(kRocksdbCacheTypeKey, "lru_cache");
  const int num_levels =
      std::max(1, ParseInt(props, "rocksdb.num_levels", 7));
  const bool force_l0 = ParseBool(
      props, "rocksdb.multi_level_cache_force_route_all_to_l0", false);
  const int cache_numshardbits = ParseInt(props, "rocksdb.cache_numshardbits", -1);
  const bool strict_capacity_limit =
      ParseBool(props, "rocksdb.cache_strict_capacity_limit", false);
  const double high_pri_pool_ratio =
      ParseDouble(props, "rocksdb.cache_high_pri_pool_ratio", 0.0);
  const double low_pri_pool_ratio =
      ParseDouble(props, "rocksdb.cache_low_pri_pool_ratio", 0.0);
  const uint32_t cache_hash_seed =
      static_cast<uint32_t>(ParseUint64(props, "rocksdb.cache_hash_seed", 0));

  // Common MLC runtime settings applied to every constructed variant.
  auto apply_mlc_common = [&props](rocksdb::MultiLevelCache& cache) {
    cache.SetSharedPoolRatio(
        ParseDouble(props, "rocksdb.multi_level_cache_shared_pool_ratio", 0.0));
    cache.SetInsertBypassCapacity(static_cast<size_t>(ParseUint64(
        props, "rocksdb.multi_level_cache_insert_bypass_capacity_bytes", 0)));
  };

  if (cache_type == "lru_cache") {
    rocksdb::LRUCacheOptions lru_opts(
        cache_capacity, cache_numshardbits, strict_capacity_limit,
        high_pri_pool_ratio, nullptr, rocksdb::kDefaultToAdaptiveMutex,
        rocksdb::kDefaultCacheMetadataChargePolicy, low_pri_pool_ratio);
    lru_opts.hash_seed = cache_hash_seed;
    auto cache = std::make_shared<rocksdb::MultiLevelCache>(
        static_cast<size_t>(num_levels), cache_capacity, lru_opts, force_l0);
    apply_mlc_common(*cache);
    if (out_multi_level_cache != nullptr) {
      *out_multi_level_cache = cache;
    }
    return cache;
  }

  if (cache_type == "cacheus_cache") {
    rocksdb::CacheusTuningOptions tuning_opts;
    tuning_opts.pending_max_age_ops = ParseUint64(
        props, "rocksdb.cache_pending_max_age_ops", 65536);
    rocksdb::LRUCacheOptions lru_opts(
        cache_capacity, cache_numshardbits, strict_capacity_limit,
        high_pri_pool_ratio, nullptr, rocksdb::kDefaultToAdaptiveMutex,
        rocksdb::kDefaultCacheMetadataChargePolicy, low_pri_pool_ratio);
    lru_opts.hash_seed = cache_hash_seed;
    auto sub_cache_factory =
        [lru_opts, tuning_opts](size_t level_capacity) mutable {
          rocksdb::LRUCacheOptions per_level_opts = lru_opts;
          per_level_opts.capacity = level_capacity;
          return rocksdb::NewCacheusCache(per_level_opts, tuning_opts);
        };
    auto cache = std::make_shared<rocksdb::MultiLevelCache>(
        static_cast<size_t>(num_levels), cache_capacity,
        std::move(sub_cache_factory), force_l0);
    apply_mlc_common(*cache);
    if (out_multi_level_cache != nullptr) {
      *out_multi_level_cache = cache;
    }
    return cache;
  }

  if (cache_type == "arc_cache") {
    rocksdb::ARCTuningOptions tuning_opts;
    tuning_opts.pending_max_age_ops = ParseUint64(
        props, "rocksdb.cache_pending_max_age_ops", 65536);
    rocksdb::LRUCacheOptions lru_opts(
        cache_capacity, cache_numshardbits, strict_capacity_limit,
        high_pri_pool_ratio, nullptr, rocksdb::kDefaultToAdaptiveMutex,
        rocksdb::kDefaultCacheMetadataChargePolicy, low_pri_pool_ratio);
    lru_opts.hash_seed = cache_hash_seed;
    auto sub_cache_factory =
        [lru_opts, tuning_opts](size_t level_capacity) mutable {
          rocksdb::LRUCacheOptions per_level_opts = lru_opts;
          per_level_opts.capacity = level_capacity;
          return rocksdb::NewARCCache(per_level_opts, tuning_opts);
        };
    auto cache = std::make_shared<rocksdb::MultiLevelCache>(
        static_cast<size_t>(num_levels), cache_capacity,
        std::move(sub_cache_factory), force_l0);
    apply_mlc_common(*cache);
    if (out_multi_level_cache != nullptr) {
      *out_multi_level_cache = cache;
    }
    return cache;
  }

  if (cache_type == "sr_hyper_clock_cache" ||
      EndsWith(cache_type, "hyper_clock_cache")) {
    size_t estimated_entry_charge = ParseUint64(
        props, "rocksdb.hcc_estimated_entry_charge", 0);
    if (cache_type == "fixed_hyper_clock_cache" && estimated_entry_charge == 0) {
      estimated_entry_charge = 4096;
    }
    rocksdb::HyperClockCacheOptions hcc_opts(
        cache_capacity, estimated_entry_charge, cache_numshardbits,
        strict_capacity_limit);
    hcc_opts.hash_seed = cache_hash_seed;
    hcc_opts.probation_insert = (cache_type == "sr_hyper_clock_cache");
    hcc_opts.frequency_aware_admission =
        ParseBool(props, "rocksdb.hcc_frequency_aware_admission", false);
    hcc_opts.freq_admission_cold_threshold = static_cast<uint32_t>(ParseUint64(
        props, "rocksdb.hcc_freq_admission_cold_threshold", 1));
    hcc_opts.freq_admission_warm_threshold = static_cast<uint32_t>(ParseUint64(
        props, "rocksdb.hcc_freq_admission_warm_threshold", 2));
    hcc_opts.freq_admission_doorkeeper =
        ParseBool(props, "rocksdb.hcc_freq_admission_doorkeeper", false);
    hcc_opts.freq_lookup_sample_log2 = static_cast<uint32_t>(ParseUint64(
        props, "rocksdb.hcc_freq_lookup_sample_log2", 1));

    const int srhcc_start_level =
        ParseInt(props, "rocksdb.multi_level_cache_srhcc_start_level", -1);
    const bool mixed_srhcc = srhcc_start_level >= 0 &&
                             srhcc_start_level < num_levels;
    // FixedHCC for bottom (hot) levels. AutoHCC degrades on a single large
    // unsharded instance under high concurrency (the "4->8GB throughput dip",
    // KNOWN_ISSUES 一.21); FixedHCC's flat preallocated table does not (global
    // FixedHCC s0: 947->999 monotonic vs AutoHCC s0 895->677). But FixedHCC
    // tables cannot grow, and MLC sizes every sub-cache table for the full
    // budget (needed for AutoHCC's mmap reservation). A FixedHCC table used at
    // a small per-level fraction is then pathologically sparse -> Evict() sweeps
    // O(table) empty slots per victim -> 64 threads spin (measured ~16x slower).
    // So apply FixedHCC only to bottom levels the allocator keeps near the full
    // budget (L6 -> ~0.88x total, dense enough); upper levels stay AutoHCC,
    // which tolerates sparsity (lazy mmap). Requires the per-level build path.
    const int fixed_start_level =
        ParseInt(props, "rocksdb.multi_level_cache_fixed_start_level", -1);
    const bool mixed_fixed = fixed_start_level >= 0 &&
                             fixed_start_level < num_levels;
    size_t fixed_entry_charge = ParseUint64(
        props, "rocksdb.multi_level_cache_fixed_entry_charge", 4096);
    if (fixed_entry_charge == 0) {
      fixed_entry_charge = 4096;
    }
    // Shard only the bottom level (L_{num_levels-1}) with the configured shard
    // bits (aligned with the lru/hcc/arc/cacheus baselines); keep every upper
    // level unsharded (num_shard_bits=0 => 1 shard). The bottom level holds the
    // bulk of the data (the allocator funds it to ~0.9x of total), so it gets
    // sharded concurrency, while the small upper levels avoid per-shard
    // fragmentation/over-sharding. Requires the per-level build path.
    const bool shard_bottom_only =
        ParseBool(props, "rocksdb.multi_level_cache_shard_bottom_only", false);
    if (!mixed_srhcc && !mixed_fixed && !shard_bottom_only) {
      auto cache = std::make_shared<rocksdb::MultiLevelCache>(
          static_cast<size_t>(num_levels), cache_capacity, hcc_opts, force_l0);
      apply_mlc_common(*cache);
      if (out_multi_level_cache != nullptr) {
        *out_multi_level_cache = cache;
      }
      return cache;
    }

    const size_t level_count = static_cast<size_t>(num_levels);
    const size_t per_level_capacity = force_l0 ? 0 : (cache_capacity / level_count);
    const size_t remainder = force_l0 ? 0 : (cache_capacity % level_count);
    std::vector<std::shared_ptr<rocksdb::Cache>> sub_caches;
    sub_caches.reserve(level_count);
    // Per-level options retained for the sparse-table rebuild factory below
    // (each level may differ in shard bits / probation / entry charge).
    std::vector<rocksdb::HyperClockCacheOptions> per_level_opts;
    per_level_opts.reserve(level_count);
    for (size_t level = 0; level < level_count; ++level) {
      size_t level_capacity = per_level_capacity + (level < remainder ? 1 : 0);
      if (force_l0 && level == 0) {
        level_capacity = cache_capacity;
      }
      rocksdb::HyperClockCacheOptions per_level = hcc_opts;
      // Auto HCC's slot array is mmap-sized once from capacity and cannot grow
      // beyond it; MLC's allocator raises a hot level far above its equal-split
      // start (L6 -> ~0.9x total), so reserve for the whole budget and set the
      // smaller starting capacity afterward. Sizing the mmap at level_capacity
      // here let Grow() run past the mapping and corrupt the heap.
      per_level.capacity = std::max<size_t>(1, cache_capacity);
      // Bottom levels -> FixedHCC (estimated_entry_charge > 0); others keep the
      // base charge (0 => AutoHCC). Table is sized from per_level.capacity (full
      // budget) via CalcHashBits, so a level the allocator funds near full stays
      // dense.
      if (mixed_fixed && level >= static_cast<size_t>(fixed_start_level)) {
        per_level.estimated_entry_charge = fixed_entry_charge;
      }
      if (level >= static_cast<size_t>(srhcc_start_level)) {
        per_level.probation_insert = true;
      }
      if (shard_bottom_only) {
        // Bottom level keeps the script-configured shard bits; others = 0.
        per_level.num_shard_bits =
            (level == level_count - 1) ? cache_numshardbits : 0;
      }
      auto sub = per_level.MakeSharedCache();
      sub->SetCapacity(level_capacity);
      sub_caches.emplace_back(std::move(sub));
      per_level_opts.push_back(per_level);
    }
    rocksdb::HyperClockCacheOptions shared_opts = hcc_opts;
    shared_opts.capacity = std::max<size_t>(1, cache_capacity);
    auto shared_cache = shared_opts.MakeSharedCache();
    shared_cache->SetCapacity(0);
    auto cache = std::make_shared<rocksdb::MultiLevelCache>(
        std::move(sub_caches), std::move(shared_cache), cache_capacity);
    // The vector<Cache> constructor cannot install the sparse-table rebuild
    // factory itself (it doesn't know how the sub-caches were built). Without
    // it, a level whose AutoHCC table grew while funded and was then defunded
    // keeps its grow-only slot array forever, and every Evict sweeps the
    // mostly-empty table (profiled at >50% of total cycles at a 1 GiB budget).
    cache->SetRebuildSubCacheFactory(
        [per_level_opts](size_t level, size_t new_capacity)
            -> std::shared_ptr<rocksdb::Cache> {
          if (level >= per_level_opts.size()) {
            return nullptr;
          }
          // Same construction as above: mmap reserved for the whole budget,
          // then shrink to the current target capacity.
          auto fresh = per_level_opts[level].MakeSharedCache();
          fresh->SetCapacity(new_capacity);
          return fresh;
        });
    apply_mlc_common(*cache);
    if (out_multi_level_cache != nullptr) {
      *out_multi_level_cache = cache;
    }
    return cache;
  }

  std::cerr << "[ycsbc-rocksdb] unsupported rocksdb.cache_type=" << cache_type
            << ", fallback to lru_cache\n";
  rocksdb::LRUCacheOptions fallback_opts(cache_capacity, cache_numshardbits,
                                         strict_capacity_limit,
                                         high_pri_pool_ratio);
  fallback_opts.hash_seed = cache_hash_seed;
  return fallback_opts.MakeSharedCache();
}

std::shared_ptr<rocksdb::Cache> CreateBlockCache(
    const utils::Properties& props,
    std::shared_ptr<rocksdb::MultiLevelCache>* out_multi_level_cache) {
  if (ParseBool(props, "rocksdb.no_block_cache", false)) {
    return nullptr;
  }
  const size_t cache_capacity = static_cast<size_t>(
      ParseUint64(props, "rocksdb.block_cache_size_bytes", 32ULL << 20));
  if (cache_capacity == 0) {
    return nullptr;
  }
  if (ParseBool(props, "rocksdb.use_multi_level_cache", false)) {
    return CreateMultiLevelCache(props, cache_capacity, out_multi_level_cache);
  }

  const std::string cache_type =
      props.GetProperty(kRocksdbCacheTypeKey, "lru_cache");
  const int cache_numshardbits = ParseInt(props, "rocksdb.cache_numshardbits", -1);
  const bool strict_capacity_limit =
      ParseBool(props, "rocksdb.cache_strict_capacity_limit", false);
  const double high_pri_pool_ratio =
      ParseDouble(props, "rocksdb.cache_high_pri_pool_ratio", 0.0);
  const double low_pri_pool_ratio =
      ParseDouble(props, "rocksdb.cache_low_pri_pool_ratio", 0.0);
  const uint32_t cache_hash_seed =
      static_cast<uint32_t>(ParseUint64(props, "rocksdb.cache_hash_seed", 0));

  if (cache_type == "lru_cache") {
    rocksdb::LRUCacheOptions lru_opts(
        cache_capacity, cache_numshardbits, strict_capacity_limit,
        high_pri_pool_ratio, nullptr, rocksdb::kDefaultToAdaptiveMutex,
        rocksdb::kDefaultCacheMetadataChargePolicy, low_pri_pool_ratio);
    lru_opts.hash_seed = cache_hash_seed;
    return lru_opts.MakeSharedCache();
  }

  if (cache_type == "cacheus_cache") {
    rocksdb::CacheusTuningOptions tuning_opts;
    tuning_opts.pending_max_age_ops = ParseUint64(
        props, "rocksdb.cache_pending_max_age_ops", 65536);
    rocksdb::LRUCacheOptions lru_opts(
        cache_capacity, cache_numshardbits, strict_capacity_limit,
        high_pri_pool_ratio, nullptr, rocksdb::kDefaultToAdaptiveMutex,
        rocksdb::kDefaultCacheMetadataChargePolicy, low_pri_pool_ratio);
    lru_opts.hash_seed = cache_hash_seed;
    return rocksdb::NewCacheusCache(lru_opts, tuning_opts);
  }

  if (cache_type == "arc_cache") {
    rocksdb::ARCTuningOptions tuning_opts;
    tuning_opts.pending_max_age_ops = ParseUint64(
        props, "rocksdb.cache_pending_max_age_ops", 65536);
    rocksdb::LRUCacheOptions lru_opts(
        cache_capacity, cache_numshardbits, strict_capacity_limit,
        high_pri_pool_ratio, nullptr, rocksdb::kDefaultToAdaptiveMutex,
        rocksdb::kDefaultCacheMetadataChargePolicy, low_pri_pool_ratio);
    lru_opts.hash_seed = cache_hash_seed;
    return rocksdb::NewARCCache(lru_opts, tuning_opts);
  }

  if (cache_type == "sr_hyper_clock_cache") {
    rocksdb::HyperClockCacheOptions opts(cache_capacity, 0, cache_numshardbits,
                                         strict_capacity_limit);
    opts.hash_seed = cache_hash_seed;
    return rocksdb::NewSRHyperClockCache(opts);
  }

  if (EndsWith(cache_type, "hyper_clock_cache")) {
    size_t estimated_entry_charge = ParseUint64(
        props, "rocksdb.hcc_estimated_entry_charge", 0);
    if (cache_type == "fixed_hyper_clock_cache" && estimated_entry_charge == 0) {
      estimated_entry_charge = 4096;
    }
    rocksdb::HyperClockCacheOptions opts(cache_capacity, estimated_entry_charge,
                                         cache_numshardbits,
                                         strict_capacity_limit);
    opts.hash_seed = cache_hash_seed;
    opts.frequency_aware_admission =
        ParseBool(props, "rocksdb.hcc_frequency_aware_admission", false);
    opts.freq_admission_cold_threshold = static_cast<uint32_t>(ParseUint64(
        props, "rocksdb.hcc_freq_admission_cold_threshold", 1));
    opts.freq_admission_warm_threshold = static_cast<uint32_t>(ParseUint64(
        props, "rocksdb.hcc_freq_admission_warm_threshold", 2));
    opts.freq_admission_doorkeeper =
        ParseBool(props, "rocksdb.hcc_freq_admission_doorkeeper", false);
    opts.freq_lookup_sample_log2 = static_cast<uint32_t>(ParseUint64(
        props, "rocksdb.hcc_freq_lookup_sample_log2", 1));
    return opts.MakeSharedCache();
  }

  std::cerr << "[ycsbc-rocksdb] unsupported rocksdb.cache_type=" << cache_type
            << ", fallback to lru_cache\n";
  rocksdb::LRUCacheOptions fallback_opts(cache_capacity, cache_numshardbits,
                                         strict_capacity_limit,
                                         high_pri_pool_ratio);
  fallback_opts.hash_seed = cache_hash_seed;
  return fallback_opts.MakeSharedCache();
}

}  // namespace

RocksdbDB::RocksdbDB(const utils::Properties& props) {
  db_path_ = props.GetProperty(kRocksdbDirKey, kDefaultRocksdbDir);
  sync_writes_ = ParseBool(props, kRocksdbSyncWriteKey, false);
  raw_kv_mode_ = ParseBool(props, "rocksdb.raw_kv_mode", false);
  raw_key_size_bytes_ = static_cast<size_t>(
      std::max<uint64_t>(1, ParseUint64(props, "rocksdb.raw_key_size_bytes", 24)));
  raw_value_size_bytes_ = static_cast<size_t>(std::max<uint64_t>(
      1, ParseUint64(props, "rocksdb.raw_value_size_bytes", 1000)));
  if (raw_kv_mode_) {
    raw_value_template_.assign(raw_value_size_bytes_, 'v');
  }

  rocksdb::Options options;
  options.create_if_missing = ParseBool(props, "rocksdb.create_if_missing", true);
  options.error_if_exists = ParseBool(props, "rocksdb.error_if_exists", false);
  options.write_buffer_size =
      static_cast<size_t>(ParseUint64(props, "rocksdb.write_buffer_size", 64ULL << 20));
  options.max_background_jobs = ParseInt(props, "rocksdb.max_background_jobs", 4);
  options.max_open_files = ParseInt(props, "rocksdb.max_open_files", -1);
  options.target_file_size_base = static_cast<uint64_t>(
      ParseUint64(props, "rocksdb.target_file_size_base", 64ULL << 20));
  // Write-stall trigger overrides, for the no-stall ablation: measured on
  // wlA the back-pressure runs ~entirely through the pending-compaction-
  // bytes slowdown, so disabling these (set triggers huge / limits to 0 =
  // unlimited) isolates how much of a cache scheme's write-side advantage
  // flows through explicit stalls vs. plain I/O contention. Values <0 (for
  // triggers) or absent keep RocksDB defaults. NOTE: must be applied AFTER
  // OptimizeLevelStyleCompaction(), which overwrites them.
  const int l0_slowdown_override =
      ParseInt(props, "rocksdb.level0_slowdown_writes_trigger", -999);
  const int l0_stop_override =
      ParseInt(props, "rocksdb.level0_stop_writes_trigger", -999);
  constexpr uint64_t kPendingLimitUnset = std::numeric_limits<uint64_t>::max();
  const uint64_t soft_pending_override = ParseUint64(
      props, "rocksdb.soft_pending_compaction_bytes_limit", kPendingLimitUnset);
  const uint64_t hard_pending_override = ParseUint64(
      props, "rocksdb.hard_pending_compaction_bytes_limit", kPendingLimitUnset);
  options.compression = ParseCompressionType(
      props.GetProperty("rocksdb.compression", "none"));
  options.use_direct_reads = ParseBool(props, "rocksdb.use_direct_reads", false);
  options.use_direct_io_for_flush_and_compaction = ParseBool(
      props, "rocksdb.use_direct_io_for_flush_and_compaction", false);
  options.allow_mmap_reads = ParseBool(props, "rocksdb.allow_mmap_reads", false);
  wait_for_compact_before_txn_ =
      ParseBool(props, "rocksdb.wait_for_compact_before_transactions", false);
  options.statistics = rocksdb::CreateDBStatistics();
  statistics_ = options.statistics;

  if (ParseBool(props, "rocksdb.optimize_level_style_compaction", true)) {
    options.OptimizeLevelStyleCompaction();
  }
  if (l0_slowdown_override != -999) {
    options.level0_slowdown_writes_trigger = l0_slowdown_override;
  }
  if (l0_stop_override != -999) {
    options.level0_stop_writes_trigger = l0_stop_override;
  }
  if (soft_pending_override != kPendingLimitUnset) {
    options.soft_pending_compaction_bytes_limit = soft_pending_override;
  }
  if (hard_pending_override != kPendingLimitUnset) {
    options.hard_pending_compaction_bytes_limit = hard_pending_override;
  }
  if (ParseBool(props, "rocksdb.increase_parallelism", true)) {
    const int parallelism = std::max(
        1, ParseInt(props, "rocksdb.parallelism", 0));
    options.IncreaseParallelism(parallelism);
  }

  rocksdb::BlockBasedTableOptions table_options;
  table_options.no_block_cache = ParseBool(props, "rocksdb.no_block_cache", false);
  table_options.block_cache = CreateBlockCache(props, &multi_level_cache_);
  block_cache_ = table_options.block_cache;
  table_options.cache_index_and_filter_blocks =
      ParseBool(props, "rocksdb.cache_index_and_filter_blocks", false);
  table_options.cache_index_and_filter_blocks_with_high_priority =
      ParseBool(props, "rocksdb.cache_index_and_filter_blocks_with_high_priority",
                false);
  table_options.pin_l0_filter_and_index_blocks_in_cache = ParseBool(
      props, "rocksdb.pin_l0_filter_and_index_blocks_in_cache", false);
  table_options.block_size = ParseUint64(props, "rocksdb.block_size", 4096);
  // Flush prepopulate (cache-for-compaction, supply side): blocks written by
  // memtable flush are inserted into the block cache immediately, so the
  // L0->L1 compaction that consumes them shortly after finds them resident
  // instead of re-reading them from the device. Only pays off when the cache
  // can actually HOLD flushed blocks until the merge (MLC funds L0 via the
  // stall-weighted compaction-value term); on a shared cache it evicts
  // foreground-hot blocks for one-shot flush output, so default off.
  if (ParseBool(props, "rocksdb.prepopulate_block_cache_flush_only", false)) {
    table_options.prepopulate_block_cache =
        rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly;
  }
  // Stronger variant: also insert COMPACTION outputs (at BOTTOM priority).
  // Base-level compaction outputs are both (a) the next merge's inputs and
  // (b) freshly rewritten versions of zipfian-hot ranges the foreground is
  // about to re-read; flush-only warming cannot serve either. Takes
  // precedence over the flush_only flag when both are set.
  if (ParseBool(props, "rocksdb.prepopulate_block_cache_flush_and_compaction",
                false)) {
    table_options.prepopulate_block_cache = rocksdb::BlockBasedTableOptions::
        PrepopulateBlockCache::kFlushAndCompaction;
  }

  const int bloom_bits = ParseInt(props, "rocksdb.bloom_bits_per_key", 0);
  if (bloom_bits > 0) {
    table_options.filter_policy.reset(
        rocksdb::NewBloomFilterPolicy(static_cast<double>(bloom_bits), false));
  }

  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  rocksdb::Status s = rocksdb::DB::Open(options, db_path_, &db_);
  if (!s.ok()) {
    std::cerr << "[ycsbc-rocksdb] open failed: " << s.ToString() << " path="
              << db_path_ << "\n";
    db_.reset();
    multi_level_cache_.reset();
    block_cache_.reset();
    return;
  }

  const bool enable_allocator =
      ParseBool(props, "rocksdb.multi_level_cache_auto_adjust", false);
  if (enable_allocator && multi_level_cache_ != nullptr) {
    rocksdb::MultiLevelAllocationOptions alloc_opts;
    alloc_opts.interval_ms = static_cast<uint64_t>(
        std::max(1, ParseInt(props, "rocksdb.multi_level_cache_adjust_interval_ms",
                             5000)));
    // Op-count adjustment cadence (lookups per round). Decouples the number of
    // solve/apply rounds from thread count / throughput so the converged
    // allocation (and hit ratio) no longer drifts with concurrency. 0 falls
    // back to the wall-clock interval_ms cadence.
    alloc_opts.adjust_interval_ops = ParseUint64(
        props, "rocksdb.multi_level_cache_adjust_interval_ops", 100000ULL);
    alloc_opts.smoothing_ratio =
        ParseDouble(props, "rocksdb.multi_level_cache_adjust_smoothing_ratio",
                    0.5);
    alloc_opts.min_total_change_bytes = static_cast<size_t>(ParseUint64(
        props, "rocksdb.multi_level_cache_adjust_min_change_bytes", 1ULL << 20));
    // Incremental marginal-step mode: transfer this many bytes per round
    // between the most over-provisioned and most under-provisioned level.
    // 0 reverts to the legacy global water-filling solver.
    alloc_opts.adjust_step_bytes = static_cast<size_t>(ParseUint64(
        props, "rocksdb.multi_level_cache_adjust_step_bytes",
        static_cast<uint64_t>(alloc_opts.adjust_step_bytes)));
    alloc_opts.step_min_score_ratio = ParseDouble(
        props, "rocksdb.multi_level_cache_adjust_step_min_score_ratio",
        alloc_opts.step_min_score_ratio);
    // Adaptive step acceleration: step doubles while consecutive applied
    // rounds pick the same recipient (up to step_max), resets to the base
    // step on direction change or skip. step_growth <= 1.0 disables.
    alloc_opts.step_max_bytes = static_cast<size_t>(ParseUint64(
        props, "rocksdb.multi_level_cache_adjust_step_max_bytes",
        static_cast<uint64_t>(alloc_opts.step_max_bytes)));
    alloc_opts.step_growth = ParseDouble(
        props, "rocksdb.multi_level_cache_adjust_step_growth",
        alloc_opts.step_growth);
    // Steady-state suppression: ghost-score EMA, ping-pong direction locks,
    // and Poisson-significance transfer gating.
    alloc_opts.ghost_score_ema_beta = ParseDouble(
        props, "rocksdb.multi_level_cache_ghost_score_ema_beta",
        alloc_opts.ghost_score_ema_beta);
    alloc_opts.step_direction_lock_rounds = ParseUint64(
        props, "rocksdb.multi_level_cache_step_direction_lock_rounds",
        alloc_opts.step_direction_lock_rounds);
    alloc_opts.ghost_min_recv_donor_ratio = ParseDouble(
        props, "rocksdb.multi_level_cache_ghost_min_recv_donor_ratio",
        alloc_opts.ghost_min_recv_donor_ratio);
    alloc_opts.ghost_significance_k = ParseDouble(
        props, "rocksdb.multi_level_cache_ghost_significance_k",
        alloc_opts.ghost_significance_k);
    alloc_opts.probe_after_skipped_rounds = static_cast<uint64_t>(std::max(
        0, ParseInt(props, "rocksdb.multi_level_cache_probe_after_skipped_rounds",
                    static_cast<int>(alloc_opts.probe_after_skipped_rounds))));
    alloc_opts.probe_step_divisor = static_cast<size_t>(std::max(
        1, ParseInt(props, "rocksdb.multi_level_cache_probe_step_divisor",
                    static_cast<int>(alloc_opts.probe_step_divisor))));
    alloc_opts.reversal_window_rounds = static_cast<uint64_t>(std::max(
        0, ParseInt(props, "rocksdb.multi_level_cache_reversal_window_rounds",
                    static_cast<int>(alloc_opts.reversal_window_rounds))));
    alloc_opts.reversal_lock_max_rounds = static_cast<uint64_t>(std::max(
        0, ParseInt(props, "rocksdb.multi_level_cache_reversal_lock_max_rounds",
                    static_cast<int>(alloc_opts.reversal_lock_max_rounds))));
    alloc_opts.ghost_uncached_floor_frac = ParseDouble(
        props, "rocksdb.multi_level_cache_ghost_uncached_floor_frac",
        alloc_opts.ghost_uncached_floor_frac);
    alloc_opts.ghost_normalize_by_uncached = ParseBool(
        props, "rocksdb.multi_level_cache_ghost_normalize_by_uncached",
        alloc_opts.ghost_normalize_by_uncached);
    alloc_opts.accel_cold_start_applies = static_cast<uint64_t>(std::max(
        0, ParseInt(props, "rocksdb.multi_level_cache_accel_cold_start_applies",
                    static_cast<int>(alloc_opts.accel_cold_start_applies))));
    alloc_opts.data_ema_beta = ParseDouble(
        props, "rocksdb.multi_level_cache_data_ema_beta",
        alloc_opts.data_ema_beta);
    // Usage-aware growth gate + dead-capacity reclaim: a level that has not
    // filled the capacity it holds is not a recipient, and capacity a level
    // persistently fails to fill is reclaimed as structural excess.
    alloc_opts.usage_grow_headroom = ParseDouble(
        props, "rocksdb.multi_level_cache_usage_grow_headroom",
        alloc_opts.usage_grow_headroom);
    alloc_opts.usage_reclaim_margin = ParseDouble(
        props, "rocksdb.multi_level_cache_usage_reclaim_margin",
        alloc_opts.usage_reclaim_margin);
    alloc_opts.usage_reclaim_rounds = static_cast<uint64_t>(std::max(
        0, ParseInt(props, "rocksdb.multi_level_cache_usage_reclaim_rounds",
                    static_cast<int>(alloc_opts.usage_reclaim_rounds))));
    alloc_opts.usage_bootstrap_bytes = static_cast<size_t>(ParseUint64(
        props, "rocksdb.multi_level_cache_usage_bootstrap_bytes",
        alloc_opts.usage_bootstrap_bytes));
    // Ghost (repeat-miss) marginal scoring for the incremental mode: replaces
    // the exponential-model score with a direct per-level measurement of
    // capacity-convertible miss traffic (repeat misses on recently-missed
    // keys). See MultiLevelCache::SetGhostTrackingEnabled.
    alloc_opts.use_ghost_marginal = ParseBool(
        props, "rocksdb.multi_level_cache_use_ghost_marginal", false);
    // Capture-rate scoring (segmented ghost): reuse-distance histogram of
    // repeat misses replaces the static per-byte denominators.
    alloc_opts.use_ghost_capture_rate = ParseBool(
        props, "rocksdb.multi_level_cache_use_ghost_capture_rate",
        alloc_opts.use_ghost_capture_rate);
    alloc_opts.ghost_dist_block_bytes = static_cast<size_t>(std::max(
        1, ParseInt(props, "rocksdb.multi_level_cache_ghost_dist_block_bytes",
                    static_cast<int>(alloc_opts.ghost_dist_block_bytes))));
    // In-flight duplicate-miss filter: histogram buckets at or below this
    // distance are discarded for levels holding real capacity (see
    // ghost_inflight_dist_bytes / ghost_inflight_min_cap_bytes).
    alloc_opts.ghost_inflight_dist_bytes = static_cast<size_t>(std::max(
        0,
        ParseInt(props, "rocksdb.multi_level_cache_ghost_inflight_dist_bytes",
                 static_cast<int>(alloc_opts.ghost_inflight_dist_bytes))));
    alloc_opts.ghost_inflight_min_cap_bytes = static_cast<size_t>(std::max(
        0, ParseInt(props,
                    "rocksdb.multi_level_cache_ghost_inflight_min_cap_bytes",
                    static_cast<int>(alloc_opts.ghost_inflight_min_cap_bytes))));
    // Donor retention cost: full-level donors are ranked and gated by the
    // measured per-byte hit density of their resident bytes.
    alloc_opts.donor_retention_frac = ParseDouble(
        props, "rocksdb.multi_level_cache_donor_retention_frac",
        alloc_opts.donor_retention_frac);
    // Reuse-distance decompression: corrects the (1-h) understatement of
    // measured distances that inflates high-hit levels' capture scores.
    alloc_opts.ghost_dist_decompress_max = ParseDouble(
        props, "rocksdb.multi_level_cache_ghost_dist_decompress_max",
        alloc_opts.ghost_dist_decompress_max);
    // Realization credit cap: a full level's capture score is capped at
    // frac * measured hit density, discounting promises that compaction
    // churn never lets pay out.
    alloc_opts.score_credit_frac =
        ParseDouble(props, "rocksdb.multi_level_cache_score_credit_frac",
                    alloc_opts.score_credit_frac);
    alloc_opts.score_credit_min_cap_bytes = static_cast<size_t>(std::max(
        0, ParseInt(props, "rocksdb.multi_level_cache_score_credit_min_cap_bytes",
                    static_cast<int>(alloc_opts.score_credit_min_cap_bytes))));
    // Density floor: symmetric half of the credit band; compensates the
    // ghost's churn blindness (repeats whose key dies unregistered).
    alloc_opts.score_credit_floor_frac = ParseDouble(
        props, "rocksdb.multi_level_cache_score_credit_floor_frac",
        alloc_opts.score_credit_floor_frac);
    // Compaction-value term (cache-for-compaction): stall-weighted per-byte
    // compaction miss density added to level scores. See comp_value_weight.
    alloc_opts.comp_value_weight =
        ParseDouble(props, "rocksdb.multi_level_cache_comp_value_weight",
                    alloc_opts.comp_value_weight);
    alloc_opts.stall_weight_ref =
        ParseDouble(props, "rocksdb.multi_level_cache_stall_weight_ref",
                    alloc_opts.stall_weight_ref);
    // Stall-adaptive lazy mode: when unstalled and converged, downsample
    // ghost recording and stretch full allocation rounds. Recovers most of
    // MLC's fixed overhead at low concurrency; exits on any back-pressure.
    alloc_opts.lazy_mode_enabled =
        ParseBool(props, "rocksdb.multi_level_cache_lazy_mode",
                  alloc_opts.lazy_mode_enabled);
    alloc_opts.lazy_stall_intensity_max = ParseDouble(
        props, "rocksdb.multi_level_cache_lazy_stall_intensity_max",
        alloc_opts.lazy_stall_intensity_max);
    alloc_opts.lazy_after_stable_rounds = static_cast<uint64_t>(std::max(
        1, ParseInt(props, "rocksdb.multi_level_cache_lazy_after_stable_rounds",
                    static_cast<int>(alloc_opts.lazy_after_stable_rounds))));
    alloc_opts.lazy_drift_tolerance_ratio = ParseDouble(
        props, "rocksdb.multi_level_cache_lazy_drift_tolerance_ratio",
        alloc_opts.lazy_drift_tolerance_ratio);
    alloc_opts.lazy_ghost_sample_shift = static_cast<uint32_t>(std::max(
        0, ParseInt(props, "rocksdb.multi_level_cache_lazy_ghost_sample_shift",
                    static_cast<int>(alloc_opts.lazy_ghost_sample_shift))));
    alloc_opts.lazy_round_multiplier = static_cast<uint32_t>(std::max(
        1, ParseInt(props, "rocksdb.multi_level_cache_lazy_round_multiplier",
                    static_cast<int>(alloc_opts.lazy_round_multiplier))));
    if (alloc_opts.use_ghost_marginal && multi_level_cache_ != nullptr) {
      const uint32_t ghost_slots_log2 = static_cast<uint32_t>(std::max(
          0, ParseInt(props, "rocksdb.multi_level_cache_ghost_slots_log2",
                      16)));
      multi_level_cache_->SetGhostTrackingEnabled(
          true, ghost_slots_log2,
          /*segmented=*/alloc_opts.use_ghost_capture_rate);
    }
    // Per-byte normalization of the ghost score (score = ghost_hits / D_i):
    // restores the capacity-price-per-hit ordering the raw count lacks.
    alloc_opts.ghost_normalize_by_data = ParseBool(
        props, "rocksdb.multi_level_cache_ghost_normalize_by_data",
        alloc_opts.ghost_normalize_by_data);
    alloc_opts.min_active_level_capacity_bytes = static_cast<size_t>(ParseUint64(
        props, "rocksdb.multi_level_cache_adjust_min_active_level_capacity_bytes",
        0));
    alloc_opts.compaction_shift_ratio = ParseDouble(
        props, "rocksdb.multi_level_cache_compaction_shift_ratio", 0.0);
    alloc_opts.compaction_shift_max_total_ratio = ParseDouble(
        props, "rocksdb.multi_level_cache_compaction_shift_max_total_ratio",
        0.1);
    alloc_opts.compaction_shift_debug = ParseBool(
        props, "rocksdb.multi_level_cache_compaction_shift_debug", false);
    // Data-size cap (cap a level's capacity at data_size * margin so the
    // surplus flows to undersaturated deep levels). Default matches the
    // allocator header default; exposed here for controlled A/B.
    alloc_opts.cap_at_data_size = ParseBool(
        props, "rocksdb.multi_level_cache_cap_at_data_size",
        alloc_opts.cap_at_data_size);
    alloc_opts.data_cap_margin_ratio = ParseDouble(
        props, "rocksdb.multi_level_cache_data_cap_margin_ratio",
        alloc_opts.data_cap_margin_ratio);
    // Data-share-weighted anti-starvation floor: reserve this fraction of the
    // total budget as a floor pool distributed across active levels in
    // proportion to their data size (deep levels get a proportionally larger
    // floor), with each active level also guaranteed an absolute minimum.
    // Enforced before the model-stability gate and applied even on gate-skip,
    // so a starved deep level is always relieved (breaks the doom loop where
    // the gate suppresses the corrective swing out of a starved state, which
    // otherwise leads to compaction stall -> write stall -> throughput
    // collapse under sustained write load). 0 disables.
    alloc_opts.min_active_level_capacity_ratio = ParseDouble(
        props, "rocksdb.multi_level_cache_min_active_level_capacity_ratio",
        alloc_opts.min_active_level_capacity_ratio);
    alloc_opts.min_active_level_floor_bytes = static_cast<size_t>(
        std::max(0, ParseInt(
            props, "rocksdb.multi_level_cache_min_active_level_floor_bytes",
            static_cast<int>(std::min<uint64_t>(
                alloc_opts.min_active_level_floor_bytes,
                static_cast<uint64_t>(std::numeric_limits<int>::max()))))));
    // L0-file-count gate for the floor relief: the floor only fires when L0 is
    // backing up (compaction falling behind -- the doom-loop signature), so a
    // healthy read-only workload (L0 ~1 file) is never perturbed. 0 = fire
    // whenever a level is below its floor (aggressive; perturbs read-only).
    alloc_opts.floor_relief_l0_file_threshold = static_cast<uint64_t>(
        std::max(0, ParseInt(
            props,
            "rocksdb.multi_level_cache_floor_relief_l0_file_threshold",
            static_cast<int>(std::min<uint64_t>(
                alloc_opts.floor_relief_l0_file_threshold,
                static_cast<uint64_t>(std::numeric_limits<int>::max()))))));
    // Model-stability gate: skip a round when two consecutive raw solved target
    // allocations disagree by more than this fraction of the total capacity (the
    // model signal is too noisy to act on -- typical of write-heavy / low
    // cache-to-data-ratio workloads). Holds the previous allocation and lets the
    // interval back off. 0 disables the gate.
    alloc_opts.model_stability_threshold = ParseDouble(
        props, "rocksdb.multi_level_cache_model_stability_threshold",
        alloc_opts.model_stability_threshold);
    // Ill-conditioning guard toggle: when on, the alpha inversion is skipped
    // for starved levels (capacity < 0.5% of data) to stop alpha blowing up in
    // the doom loop. With compaction excluded from alpha (foreground-only) and
    // the data-share floor as the primary anti-starvation fix, this guard is
    // redundant for the refined model -- expose it so it can be disabled.
    const bool ill_conditioning_guard_enabled = ParseBool(
        props, "rocksdb.multi_level_cache_ill_conditioning_guard", true);
    // Compaction-modeling toggles. Default = config C (refined): lambda uses
    // total lookups (compaction included, so write-path pressure keeps
    // signaling), alpha uses foreground-only (compaction excluded from the
    // hit-curve). Set lambda_exclude_compaction=true to also drop compaction
    // from the access-frequency channel -> config B (full exclusion). Provided
    // to actually measure B to completion.
    const bool lambda_exclude_compaction = ParseBool(
        props, "rocksdb.multi_level_cache_lambda_exclude_compaction", false);
    // Symmetric toggle for the alpha channel (default true = compaction
    // excluded from the hit-curve). Set false to include compaction in alpha
    // -> config A (full inclusion) when lambda_exclude_compaction is also
    // false.
    const bool alpha_exclude_compaction = ParseBool(
        props, "rocksdb.multi_level_cache_alpha_exclude_compaction", true);

    const std::string mode = props.GetProperty(
        "rocksdb.multi_level_cache_allocator_mode", "model");
    if (mode == "baseline_emulation") {
      alloc_opts.mode = rocksdb::MultiLevelAllocatorMode::kBaselineEmulation;
    } else {
      alloc_opts.mode = rocksdb::MultiLevelAllocatorMode::kModel;
    }

    // Estimator semantics ported from db_bench (tools/db_bench_tool.cc):
    //   constant_one    : alpha = 1, D = window-smoothed raw level data size.
    //   robust_hit_rate : alpha derived by exactly inverting the model hit
    //                     curve hit = 1 - exp(-alpha*c/D) at the observed
    //                     (capacity, hit_rate) point, with confidence
    //                     shrinkage to a prior and EMA smoothing.
    // Additionally, use_effective_data_size replaces D with an effective
    // working-set estimate D_eff = -(alpha*c)/ln(1-hit), confidence-blended
    // with raw data and clamped to [0.01*D_raw, D_raw].
    // The shadow_cache estimator from db_bench is not ported.
    const std::string alpha_estimator = props.GetProperty(
        "rocksdb.multi_level_cache_alpha_estimator", "constant_one");
    const double alpha_floor = ParseDouble(
        props, "rocksdb.multi_level_cache_alpha_floor", 0.1);
    const double alpha_max = ParseDouble(
        props, "rocksdb.multi_level_cache_alpha_max", 100.0);
    const bool use_effective_data_size = ParseBool(
        props, "rocksdb.multi_level_cache_use_effective_data_size", false);
    // Enable per-level HLL working-set (distinct-block) tracking without driving
    // the saturation scale, for the reuse diagnostic (MLC_WSS_DIAG). Lets us
    // study reuse structure without perturbing allocation.
    const bool track_working_set = ParseBool(
        props, "rocksdb.multi_level_cache_track_working_set", false);
    // Reuse-channel model fix (finalized default for product MLC schemes): set
    // lambda to the cacheable (reusable) access rate lambda_i = max(eps,
    // fg_lookups_i - distinct_i) instead of raw lookups. High-traffic/no-reuse
    // deep levels (fg_lookups ~= distinct) then get lambda ~= 0 -> marginal gain
    // a_i = lambda*alpha/D ~= 0 -> the solver starves them from the model
    // itself, independent of D and of current capacity (no doom loop). Evidence:
    // reuse_ratio cleanly separates useful L3/L4 (0.11-0.18) from useless L5/L6
    // (~0). Keeps D physical. distinct is estimated by the per-level HLL.
    const bool use_reuse_lambda = ParseBool(
        props, "rocksdb.multi_level_cache_use_reuse_lambda", false);
    const uint32_t working_set_sample_shift = static_cast<uint32_t>(std::max(
        0, ParseInt(props,
                     "rocksdb.multi_level_cache_working_set_sample_shift", 0)));
    if ((track_working_set || use_reuse_lambda) &&
        multi_level_cache_ != nullptr) {
      multi_level_cache_->SetWorkingSetTrackingEnabled(
          true, working_set_sample_shift);
    }
    const size_t alpha_window_rounds = static_cast<size_t>(std::max(
        1, ParseInt(props, "rocksdb.multi_level_cache_alpha_window_rounds", 5)));
    // Use the latest data-size sample (no window averaging): rounds=1 keeps only
    // the most recent sample, and the time-weighted average of a single sample
    // equals that sample's instantaneous value. The model-stability gate now
    // provides the stability that the running mean used to, so we feed the
    // freshest level data size straight through. data_size_window_ms is then
    // irrelevant (only one sample is ever retained).
    const size_t data_window_rounds = static_cast<size_t>(std::max(
        1,
        ParseInt(props, "rocksdb.multi_level_cache_data_size_window_rounds", 1)));
    const uint64_t data_window_us =
        static_cast<uint64_t>(std::max(
            1, ParseInt(props, "rocksdb.multi_level_cache_data_size_window_ms",
                        30000))) *
        1000;
    const bool use_constant_one_alpha = alpha_estimator != "robust_hit_rate";

    multi_level_allocator_ = std::make_unique<rocksdb::MultiLevelCacheAllocator>(
        multi_level_cache_,
        [cache = multi_level_cache_, db = db_.get(), stats = statistics_.get(),
         alpha_floor, alpha_max,
         use_effective_data_size, track_working_set,
         use_reuse_lambda, alpha_window_rounds, data_window_rounds,
         data_window_us, use_constant_one_alpha, ill_conditioning_guard_enabled,
         lambda_exclude_compaction, alpha_exclude_compaction,
         prev_lookups = std::vector<uint64_t>{},
         prev_hits = std::vector<uint64_t>{},
         prev_fg_lookups = std::vector<uint64_t>{},
         prev_fg_hits = std::vector<uint64_t>{},
         prev_alpha = std::vector<double>{},
         prev_effective_data = std::vector<double>{},
         observed_history =
             std::vector<std::deque<std::pair<uint64_t, uint64_t>>>{},
         raw_data_history =
             std::vector<std::deque<std::pair<uint64_t, double>>>{}](
            std::vector<double>* lambda, std::vector<double>* data,
            std::vector<double>* alpha, uint64_t* l0_file_count,
            uint64_t* stall_micros) mutable {
          if (cache == nullptr || lambda == nullptr || data == nullptr ||
              alpha == nullptr) {
            return false;
          }
          // L0 SST file count = compaction-backlog signal for the floor relief
          // gate. Read every round (cheap int property). 0 on any failure or
          // when the DB handle is null (keeps the gate conservative-off).
          if (l0_file_count != nullptr) {
            uint64_t v = 0;
            if (db != nullptr) {
              db->GetIntProperty("rocksdb.num-files-at-level0", &v);
            }
            *l0_file_count = v;
          }
          // Cumulative write-stall micros: the allocator differentiates this
          // into the stall-intensity weight for the compaction-value term
          // (converting compaction reads only buys throughput when the
          // workload is stall-bound). Cheap relaxed ticker read.
          if (stall_micros != nullptr) {
            *stall_micros =
                stats != nullptr
                    ? stats->getTickerCount(rocksdb::STALL_MICROS)
                    : 0;
          }
          const auto stats = cache->GetLevelMetricsSnapshot();
          const size_t level_count = stats.lookups.size();
          if (level_count == 0 || stats.hits.size() != level_count ||
              stats.fg_lookups.size() != level_count ||
              stats.fg_hits.size() != level_count ||
              stats.capacities.size() != level_count ||
              stats.data_sizes.size() != level_count) {
            return false;
          }
          if (prev_lookups.size() != level_count ||
              prev_hits.size() != level_count ||
              prev_fg_lookups.size() != level_count ||
              prev_fg_hits.size() != level_count ||
              prev_alpha.size() != level_count ||
              prev_effective_data.size() != level_count ||
              observed_history.size() != level_count ||
              raw_data_history.size() != level_count) {
            prev_lookups = stats.lookups;
            prev_hits = stats.hits;
            prev_fg_lookups = stats.fg_lookups;
            prev_fg_hits = stats.fg_hits;
            prev_alpha.assign(level_count, 1.0);
            prev_effective_data.assign(level_count, 1.0);
            if (use_reuse_lambda || track_working_set) {
              // Prime the sketch so the first real round's distinct count covers
              // the same window as the first real delta_fg_lookups (otherwise the
              // first reuse-lambda would subtract a longer-window distinct and be
              // spuriously conservative).
              cache->DrainForegroundWorkingSetDistinct();
            }
            for (size_t level = 0; level < level_count; ++level) {
              prev_effective_data[level] =
                  stats.data_sizes[level] > 0
                      ? static_cast<double>(stats.data_sizes[level])
                      : 1.0;
            }
            observed_history.assign(
                level_count, std::deque<std::pair<uint64_t, uint64_t>>{});
            raw_data_history.assign(level_count,
                                    std::deque<std::pair<uint64_t, double>>{});
            return false;
          }

          lambda->assign(level_count, 1.0);
          data->assign(level_count, 1.0);
          alpha->assign(level_count, 1.0);

          // Levels with no on-disk data report D = 0 (inactive). They used to
          // be substituted with the mean of the observed levels' data sizes
          // (a legacy convenience for the water-filling solver), which handed
          // empty levels a ~26 GiB phantom footprint at the 100 GiB dataset:
          // the allocator then granted them data-share floors (~14 MiB each
          // on the three empty upper levels) and recipient eligibility, and
          // the phantom mass diluted every real level's floor share. The
          // solver treats D = 0 levels as non-fundable, and the incremental
          // allocator's floors/upper-bounds/score paths all key off D > 0,
          // so zero is the semantically correct encoding for "no data".

          // Per-level distinct foreground blocks this window from the per-level
          // HLL. Consumed by the reuse-lambda path below (lambda_i = max(eps,
          // fg_lookups_i - distinct_i)). Empty when tracking is off. Draining
          // also resets the sketch for the next window.
          std::vector<double> ws_distinct;
          if (track_working_set || use_reuse_lambda) {
            ws_distinct = cache->DrainForegroundWorkingSetDistinct();
            const std::vector<double>& distinct = ws_distinct;
            if (distinct.size() == level_count) {
              // Reuse diagnostic: per level, contrast the foreground access
              // volume against the distinct footprint and realized hits.
              // Computed here because prev_fg_* are still the previous window's
              // values.
              static const bool kWssDiag = getenv("MLC_WSS_DIAG") != nullptr;
              if (kWssDiag) {
                std::string line;
                for (size_t level = 0; level < level_count; ++level) {
                  const uint64_t fgl =
                      stats.fg_lookups[level] >= prev_fg_lookups[level]
                          ? stats.fg_lookups[level] - prev_fg_lookups[level]
                          : 0;
                  const uint64_t fgh =
                      stats.fg_hits[level] >= prev_fg_hits[level]
                          ? stats.fg_hits[level] - prev_fg_hits[level]
                          : 0;
                  const double dist = std::max(0.0, distinct[level]);
                  // reuse_ratio = 1 - distinct/fg_lookups: fraction of
                  // foreground accesses that are repeats within the window.
                  const double reuse =
                      fgl > 0 ? 1.0 - dist / static_cast<double>(fgl) : 0.0;
                  char buf[160];
                  snprintf(buf, sizeof(buf),
                           "L%zu[fgl=%llu fgh=%llu dist=%.0f reuse=%.3f dMiB=%llu] ",
                           level, (unsigned long long)fgl,
                           (unsigned long long)fgh, dist, reuse,
                           (unsigned long long)(stats.data_sizes[level] >> 20));
                  line += buf;
                }
                fprintf(stderr, "[MLC_WSS] %s\n", line.c_str());
              }
            }
          }

          constexpr double kLambdaEpsilon = 1e-6;
          constexpr double kAlphaPrior = 1.0;
          constexpr uint64_t kAlphaConfidenceLookups = 5000;
          constexpr double kAlphaEmaBeta = 0.2;
          constexpr uint64_t kDataConfidenceLookups = 20000;
          constexpr double kDataEmaBeta = 0.25;
          constexpr double kDataDriftBetaNoObs = 0.05;
          constexpr double kDataMinFractionOfRaw = 0.01;
          constexpr double kMinMissRate = 1e-6;
          constexpr double kMaxHitRate = 1.0 - kMinMissRate;
          const uint64_t now_us = static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now().time_since_epoch())
                  .count());
          const uint64_t data_window_start =
              now_us > data_window_us ? now_us - data_window_us : 0;
          for (size_t level = 0; level < level_count; ++level) {
            const uint64_t curr_lookups = stats.lookups[level];
            const uint64_t curr_hits = stats.hits[level];
            const uint64_t delta_lookups =
                curr_lookups >= prev_lookups[level]
                    ? curr_lookups - prev_lookups[level]
                    : 0;
            const uint64_t delta_hits =
                curr_hits >= prev_hits[level] ? curr_hits - prev_hits[level] : 0;
            // Foreground-only deltas drive alpha (hit-curve shape): compaction
            // reads are streaming/one-shot, so their hits/misses must not
            // dilute the foreground hit rate. Total deltas still drive lambda
            // (access frequency) so compaction activity keeps signaling
            // write-path pressure on levels like L0.
            const uint64_t curr_fg_lookups = stats.fg_lookups[level];
            const uint64_t curr_fg_hits = stats.fg_hits[level];
            const uint64_t delta_fg_lookups =
                curr_fg_lookups >= prev_fg_lookups[level]
                    ? curr_fg_lookups - prev_fg_lookups[level]
                    : 0;
            const uint64_t delta_fg_hits =
                curr_fg_hits >= prev_fg_hits[level]
                    ? curr_fg_hits - prev_fg_hits[level]
                    : 0;

            // config B drops compaction from the frequency channel too.
            const uint64_t lambda_delta =
                lambda_exclude_compaction ? delta_fg_lookups : delta_lookups;
            if (use_reuse_lambda && ws_distinct.size() == level_count) {
              // Reuse-channel: lambda = cacheable (reusable) access rate =
              // foreground accesses minus the distinct footprint touched this
              // window. Near-zero for one-shot/no-reuse deep levels -> the
              // model starves them via a vanishing marginal gain, without
              // touching D or the capacity feedback loop.
              const double reusable =
                  static_cast<double>(delta_fg_lookups) - ws_distinct[level];
              (*lambda)[level] =
                  reusable > 0.0 ? reusable : kLambdaEpsilon;
            } else {
              (*lambda)[level] = lambda_delta > 0
                                     ? static_cast<double>(lambda_delta)
                                     : kLambdaEpsilon;
            }
            // Alpha channel: foreground-only by default (compaction excluded);
            // set alpha_exclude_compaction=false to feed total deltas (config A).
            const uint64_t alpha_delta_lookups =
                alpha_exclude_compaction ? delta_fg_lookups : delta_lookups;
            const uint64_t alpha_delta_hits =
                alpha_exclude_compaction ? delta_fg_hits : delta_hits;
            const double raw_level_data_instant =
                static_cast<double>(stats.data_sizes[level]);
            auto& raw_history = raw_data_history[level];
            raw_history.push_back({now_us, raw_level_data_instant});
            while (raw_history.size() >= 2 &&
                   raw_history[1].first <= data_window_start) {
              raw_history.pop_front();
            }
            while (raw_history.size() > data_window_rounds) {
              raw_history.pop_front();
            }
            double raw_level_data = raw_level_data_instant;
            if (!raw_history.empty() && now_us > data_window_start) {
              uint64_t prev_ts = data_window_start;
              double prev_val = raw_history.front().second;
              double weighted_sum = 0.0;
              for (const auto& sample : raw_history) {
                const uint64_t sample_ts = std::min(now_us, sample.first);
                if (sample_ts > prev_ts) {
                  weighted_sum +=
                      prev_val * static_cast<double>(sample_ts - prev_ts);
                }
                prev_ts = std::max(prev_ts, sample_ts);
                prev_val = sample.second;
              }
              if (now_us > prev_ts) {
                weighted_sum += prev_val * static_cast<double>(now_us - prev_ts);
              }
              const double denom =
                  static_cast<double>(now_us - data_window_start);
              if (denom > 0.0) {
                raw_level_data = weighted_sum / denom;
              }
            }
            // The model's saturation scale D is the on-disk data size (raw or
            // effective-data path below), used consistently in both the alpha
            // inversion and the capacity solve so the MRC stays self-consistent.
            const double model_scale = raw_level_data;
            if (use_constant_one_alpha) {
              (*alpha)[level] = 1.0;
              (*data)[level] = model_scale;
              prev_alpha[level] = 1.0;
              prev_effective_data[level] = model_scale;
              continue;
            }

            // Robust online alpha estimation:
            //   1) derive raw alpha from observed window hit rate
            //   2) confidence-shrink to prior under small samples
            //   3) EMA smooth across windows
            double derived_alpha = prev_alpha[level];
            const size_t capacity_bytes = stats.capacities[level];
            double observed_hit_rate = -1.0;
            // Ill-conditioning guard: the inversion derived_alpha = -(D/c) *
            // log(miss) blows up when the level's cache capacity c is a tiny
            // fraction of its data size D (a starved deep level). The D/c
            // multiplier turns small miss-rate perturbations into large,
            // oscillating alpha estimates that the model-stability gate then
            // suppresses, freezing the level in its starved state (doom loop).
            // Skip the inversion entirely in the starved regime and collapse
            // confidence to 0 (fall back to the prior). The data-share floor
            // guarantees the level still receives capacity; once it recovers
            // above the 0.5% threshold the inversion resumes. Belt-and-
            // suspenders -- the floor is the primary fix. Toggleable because
            // with compaction excluded from alpha the guard is redundant.
            const bool starved =
                ill_conditioning_guard_enabled && capacity_bytes > 0 &&
                model_scale > 0.0 &&
                static_cast<double>(capacity_bytes) <
                    0.005 * model_scale;
            if (!starved && alpha_delta_lookups > 0 && capacity_bytes > 0 &&
                model_scale > 0.0) {
              observed_history[level].push_back(
                  {alpha_delta_hits, alpha_delta_lookups});
              while (observed_history[level].size() > alpha_window_rounds) {
                observed_history[level].pop_front();
              }
              uint64_t observed_hits_sum = 0;
              uint64_t observed_lookups_sum = 0;
              for (const auto& p : observed_history[level]) {
                observed_hits_sum += p.first;
                observed_lookups_sum += p.second;
              }
              observed_hit_rate = std::min(
                  kMaxHitRate,
                  std::max(0.0, static_cast<double>(observed_hits_sum) /
                                    static_cast<double>(observed_lookups_sum)));
              const double miss_rate =
                  std::max(kMinMissRate, 1.0 - observed_hit_rate);
              derived_alpha =
                  -(model_scale / static_cast<double>(capacity_bytes)) *
                  std::log(miss_rate);
            }

            const double confidence =
                starved
                    ? 0.0
                    : std::min(1.0, static_cast<double>(alpha_delta_lookups) /
                                        static_cast<double>(kAlphaConfidenceLookups));
            const double shrunk_alpha =
                confidence * derived_alpha + (1.0 - confidence) * kAlphaPrior;
            const double smoothed_alpha =
                (1.0 - kAlphaEmaBeta) * prev_alpha[level] +
                kAlphaEmaBeta * shrunk_alpha;
            double final_alpha = smoothed_alpha;
            if (final_alpha < alpha_floor) {
              final_alpha = alpha_floor;
            } else if (final_alpha > alpha_max) {
              final_alpha = alpha_max;
            }
            (*alpha)[level] = final_alpha;
            prev_alpha[level] = final_alpha;

            const double previous_effective_data =
                prev_effective_data[level] > 0.0 ? prev_effective_data[level]
                                                 : raw_level_data;
            const bool has_observed_hit = observed_hit_rate >= 0.0;
            double target_effective_data = raw_level_data;
            if (has_observed_hit && capacity_bytes > 0 && final_alpha > 0.0) {
              const double miss_rate =
                  std::max(kMinMissRate, 1.0 - observed_hit_rate);
              const double observed_effective_data =
                  -(final_alpha * static_cast<double>(capacity_bytes)) /
                  std::log(miss_rate);
              if (std::isfinite(observed_effective_data) &&
                  observed_effective_data > 0.0) {
                // Confidence in the observed effective-data estimate scales
                // with the sample count that produced observed_hit_rate, which
                // comes from the alpha channel (foreground-only by default) --
                // not the total lookups. Using total would overstate confidence
                // when compaction dominates a level's traffic.
                const double data_confidence = std::min(
                    1.0, static_cast<double>(alpha_delta_lookups) /
                             static_cast<double>(kDataConfidenceLookups));
                target_effective_data =
                    data_confidence * observed_effective_data +
                    (1.0 - data_confidence) * raw_level_data;
              }
            }
            const double data_beta =
                has_observed_hit ? kDataEmaBeta : kDataDriftBetaNoObs;
            double effective_level_data =
                previous_effective_data +
                data_beta * (target_effective_data - previous_effective_data);
            const double data_lower_bound =
                std::max(1.0, raw_level_data * kDataMinFractionOfRaw);
            const double data_upper_bound =
                std::max(raw_level_data, data_lower_bound);
            effective_level_data =
                std::max(data_lower_bound,
                         std::min(effective_level_data, data_upper_bound));
            (*data)[level] = use_effective_data_size ? effective_level_data
                                                     : raw_level_data;
            prev_effective_data[level] = effective_level_data;
          }
          prev_lookups = stats.lookups;
          prev_hits = stats.hits;
          prev_fg_lookups = stats.fg_lookups;
          prev_fg_hits = stats.fg_hits;
          return true;
        },
        alloc_opts);
    multi_level_allocator_->Start();
  }

  if (multi_level_cache_ != nullptr &&
      ParseBool(props, "rocksdb.multi_level_cache_dynamic_srhcc_enable", false)) {
    const int check_interval_ops = std::max(
        64, ParseInt(props, "rocksdb.multi_level_cache_dynamic_srhcc_check_interval_ops",
                     4096));
    const int min_samples = std::max(
        8, ParseInt(props, "rocksdb.multi_level_cache_dynamic_srhcc_min_samples", 64));
    const int sample_rate_log2 = std::max(
        0, ParseInt(props, "rocksdb.multi_level_cache_dynamic_srhcc_sample_rate_log2",
                    7));
    const int poll_interval_ms = std::max(
        10, ParseInt(props, "rocksdb.multi_level_cache_dynamic_srhcc_poll_interval_ms",
                     200));
    const double unique_ratio_enable_threshold = ParseDouble(
        props,
        "rocksdb.multi_level_cache_dynamic_srhcc_unique_ratio_enable_threshold",
        ParseDouble(props,
                    "rocksdb.multi_level_cache_dynamic_srhcc_unique_ratio_threshold",
                    0.50));
    const double unique_ratio_disable_threshold = ParseDouble(
        props,
        "rocksdb.multi_level_cache_dynamic_srhcc_unique_ratio_disable_threshold",
        0.30);
    multi_level_cache_->ConfigureDynamicSRHCC(
        true, static_cast<uint32_t>(check_interval_ops),
        static_cast<uint32_t>(min_samples), unique_ratio_enable_threshold,
        unique_ratio_disable_threshold, static_cast<uint32_t>(sample_rate_log2),
        static_cast<uint32_t>(poll_interval_ms));
  }
}

RocksdbDB::~RocksdbDB() {
  if (multi_level_allocator_ != nullptr) {
    multi_level_allocator_->Stop();
    multi_level_allocator_.reset();
  }
  if (statistics_ != nullptr) {
    const uint64_t hits =
        statistics_->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
    const uint64_t misses =
        statistics_->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
    const uint64_t total = hits + misses;
    const double hit_ratio = total > 0
                                 ? (static_cast<double>(hits) /
                                    static_cast<double>(total))
                                 : 0.0;
    std::cerr << "# Block cache stats" << std::endl;
    std::cerr << "rocksdb\tcache_hit\t" << hits << std::endl;
    std::cerr << "rocksdb\tcache_miss\t" << misses << std::endl;
    std::cerr << "rocksdb\tcache_hit_ratio\t" << hit_ratio << std::endl;

    // Foreground-only hit ratio: same block-cache events as above but with
    // compaction-induced lookups excluded (see BLOCK_CACHE_FOREGROUND_* /
    // MLCLookupIsCompaction). The overall cache_hit_ratio is diluted by
    // streaming compaction reads that carry no foreground query value; this
    // number is the compaction-free hit ratio and is emitted uniformly for
    // every cache type (LRU / HyperClockCache / MultiLevelCache).
    const uint64_t fg_hits =
        statistics_->getTickerCount(rocksdb::BLOCK_CACHE_FOREGROUND_HIT);
    const uint64_t fg_misses =
        statistics_->getTickerCount(rocksdb::BLOCK_CACHE_FOREGROUND_MISS);
    const uint64_t fg_total = fg_hits + fg_misses;
    const double fg_hit_ratio =
        fg_total > 0
            ? (static_cast<double>(fg_hits) / static_cast<double>(fg_total))
            : 0.0;
    std::cerr << "rocksdb\tcache_fg_hit\t" << fg_hits << std::endl;
    std::cerr << "rocksdb\tcache_fg_miss\t" << fg_misses << std::endl;
    std::cerr << "rocksdb\tcache_fg_hit_ratio\t" << fg_hit_ratio << std::endl;

    const auto emit_typed_ratio = [&](const char* name,
                                      rocksdb::Tickers hit_ticker,
                                      rocksdb::Tickers miss_ticker) {
      const uint64_t type_hits = statistics_->getTickerCount(hit_ticker);
      const uint64_t type_misses = statistics_->getTickerCount(miss_ticker);
      const uint64_t type_total = type_hits + type_misses;
      const double type_ratio =
          type_total > 0
              ? (static_cast<double>(type_hits) /
                 static_cast<double>(type_total))
              : 0.0;
      std::cerr << "rocksdb\tcache_" << name << "_hit\t" << type_hits
                << std::endl;
      std::cerr << "rocksdb\tcache_" << name << "_miss\t" << type_misses
                << std::endl;
      std::cerr << "rocksdb\tcache_" << name << "_hit_ratio\t" << type_ratio
                << std::endl;
    };
    emit_typed_ratio("data", rocksdb::BLOCK_CACHE_DATA_HIT,
                     rocksdb::BLOCK_CACHE_DATA_MISS);
    emit_typed_ratio("filter", rocksdb::BLOCK_CACHE_FILTER_HIT,
                     rocksdb::BLOCK_CACHE_FILTER_MISS);
    emit_typed_ratio("index", rocksdb::BLOCK_CACHE_INDEX_HIT,
                     rocksdb::BLOCK_CACHE_INDEX_MISS);

    // Write-stall accounting: total microseconds writers spent blocked on
    // compaction back-pressure during the transaction phase (statistics_ is
    // Reset() before it). This is the channel through which the cache scheme
    // affects the WRITE path in a 50/50 workload -- compaction speed ->
    // L0/pending-bytes back-pressure -> stall -- and thus the metric that
    // decides whether a throughput gap between schemes comes from the write
    // side rather than from block-cache hit ratio.
    std::cerr << "rocksdb\tstall_micros\t"
              << statistics_->getTickerCount(rocksdb::STALL_MICROS)
              << std::endl;

    // Stall-cause breakdown. stall_micros only accounts EXPLICIT write
    // delays/stops, which measured ~1us/write of the observed 52us/write
    // p50 gap between schemes on wlA -- the bulk of the write-side
    // difference flows through unaccounted channels (delayed-write rate
    // limiting shifting the whole latency distribution, and group-commit
    // queueing behind compaction bursts). The io_stalls.* counters from
    // cfstats split stall EVENTS by trigger (L0 file count, pending
    // compaction bytes, memtable count; *_slowdown vs *_stop), which
    // identifies WHICH back-pressure mechanism a cache scheme relieves.
    // db_ can be null here if DB::Open failed (constructor resets it and
    // returns early while statistics_ stays set); the map-property reads
    // below must not dereference it.
    if (db_ != nullptr) {
      // kCFWriteStallStats / kDBWriteStallStats: per-(cause, condition)
      // event counts, e.g. l0-file-count-limit delays/stops,
      // pending-compaction-bytes delays/stops, memtable-limit stalls.
      // Key names arrive hyphenated (e.g. pending-compaction-bytes-delays);
      // normalize to underscores so the matrix runner's metric regex
      // ([a-zA-Z0-9_]+) picks them up instead of silently skipping them.
      const auto sanitize = [](std::string name) {
        for (char& c : name) {
          if (!std::isalnum(static_cast<unsigned char>(c))) {
            c = '_';
          }
        }
        return name;
      };
      std::map<std::string, std::string> ws;
      if (db_->GetMapProperty(rocksdb::DB::Properties::kCFWriteStallStats,
                              &ws)) {
        for (const auto& kv : ws) {
          std::cerr << "rocksdb\tstallcf_" << sanitize(kv.first) << "\t"
                    << kv.second << std::endl;
        }
      }
      ws.clear();
      if (db_->GetMapProperty(rocksdb::DB::Properties::kDBWriteStallStats,
                              &ws)) {
        for (const auto& kv : ws) {
          std::cerr << "rocksdb\tstalldb_" << sanitize(kv.first) << "\t"
                    << kv.second << std::endl;
        }
      }
      // Delayed-write rate limiter state at run end (bytes/s the write
      // controller currently allows; 0 or absent means no active delay).
      uint64_t adwr = 0;
      if (db_->GetIntProperty("rocksdb.actual-delayed-write-rate", &adwr)) {
        std::cerr << "rocksdb\tactual_delayed_write_rate\t" << adwr
                  << std::endl;
      }
      // Per-stall-event duration distribution (WRITE_STALL histogram):
      // count tells how OFTEN back-pressure engaged, mean how hard.
      rocksdb::HistogramData hs;
      statistics_->histogramData(rocksdb::WRITE_STALL, &hs);
      std::cerr << "rocksdb\twrite_stall_hist_count\t"
                << static_cast<uint64_t>(hs.count) << std::endl;
      std::cerr << "rocksdb\twrite_stall_hist_mean_us\t" << hs.average
                << std::endl;
      std::cerr << "rocksdb\twrite_stall_hist_p99_us\t" << hs.percentile99
                << std::endl;
      // Compaction volume: total bytes read/written by compaction during
      // the transaction phase. Faster/cheaper compaction is the upstream
      // of every stall channel, so schemes are compared on equal terms.
      std::cerr << "rocksdb\tcompact_read_bytes\t"
                << statistics_->getTickerCount(rocksdb::COMPACT_READ_BYTES)
                << std::endl;
      std::cerr << "rocksdb\tcompact_write_bytes\t"
                << statistics_->getTickerCount(rocksdb::COMPACT_WRITE_BYTES)
                << std::endl;
      // WAL sync/write volume for the group-commit channel.
      std::cerr << "rocksdb\twrite_with_wal\t"
                << statistics_->getTickerCount(rocksdb::WRITE_WITH_WAL)
                << std::endl;
      rocksdb::HistogramData hw;
      statistics_->histogramData(rocksdb::COMPACTION_TIME, &hw);
      std::cerr << "rocksdb\tcompaction_time_count\t"
                << static_cast<uint64_t>(hw.count) << std::endl;
      std::cerr << "rocksdb\tcompaction_time_mean_us\t" << hw.average
                << std::endl;
    }

    // Per-operation latency percentiles (microseconds) from RocksDB's own
    // DB_GET / DB_WRITE histograms. These are recorded at the default stats
    // level with no added per-op cost, and statistics_ was Reset() right before
    // the transaction phase, so they reflect only measured transactions. Tail
    // latency (p99/max) is the metric where a higher cache hit ratio pays off
    // most visibly even when mean throughput is I/O-bound: a hit is served from
    // memory while a miss pays the NVMe + kernel-queue tail.
    const auto emit_latency = [&](const char* name, rocksdb::Histograms hist) {
      rocksdb::HistogramData h;
      statistics_->histogramData(hist, &h);
      std::cerr << "rocksdb\t" << name << "_count\t" << h.count << std::endl;
      std::cerr << "rocksdb\t" << name << "_us_mean\t" << h.average << std::endl;
      std::cerr << "rocksdb\t" << name << "_us_p50\t" << h.median << std::endl;
      std::cerr << "rocksdb\t" << name << "_us_p95\t" << h.percentile95
                << std::endl;
      std::cerr << "rocksdb\t" << name << "_us_p99\t" << h.percentile99
                << std::endl;
      std::cerr << "rocksdb\t" << name << "_us_max\t" << h.max << std::endl;
    };
    emit_latency("lat_get", rocksdb::DB_GET);
    emit_latency("lat_write", rocksdb::DB_WRITE);
    // Whole-scan latency (Seek + Nexts + decode), recorded client-side in
    // Scan() into the otherwise-unused DB_MULTIGET slot. Zero count on
    // non-scan workloads.
    emit_latency("lat_scan", rocksdb::DB_MULTIGET);
  }
  std::cerr << "rocksdb\tinsert_get_ok\t" << insert_get_ok_count_ << std::endl;
  std::cerr << "rocksdb\tinsert_get_not_found\t" << insert_get_not_found_count_
            << std::endl;
  std::cerr << "rocksdb\tinsert_get_other\t" << insert_get_other_count_
            << std::endl;
  std::cerr << "rocksdb\tinsert_put_ok\t" << insert_put_ok_count_ << std::endl;
  std::cerr << "rocksdb\tinsert_put_fail\t" << insert_put_fail_count_
            << std::endl;
  if (!last_insert_get_other_status_.empty()) {
    std::cerr << "rocksdb\tinsert_get_other_last_status\t"
              << last_insert_get_other_status_ << std::endl;
  }
  if (!last_insert_put_fail_status_.empty()) {
    std::cerr << "rocksdb\tinsert_put_fail_last_status\t"
              << last_insert_put_fail_status_ << std::endl;
  }
  EmitNumericCacheOptionsAsMetrics(block_cache_);
  if (multi_level_cache_ != nullptr) {
    const auto snapshot = multi_level_cache_->GetLevelMetricsSnapshot();
    uint64_t total_lookups = 0;
    uint64_t total_hits = 0;
    uint64_t total_fg_lookups = 0;
    uint64_t total_fg_hits = 0;
    for (size_t i = 0; i < snapshot.lookups.size(); ++i) {
      const uint64_t lookups = snapshot.lookups[i];
      const uint64_t hits = snapshot.hits[i];
      total_lookups += lookups;
      total_hits += hits;
      const double level_hit_ratio =
          lookups > 0 ? static_cast<double>(hits) / static_cast<double>(lookups)
                      : 0.0;
      std::cerr << "rocksdb\tmlc_level_" << i << "_lookups\t" << lookups
                << std::endl;
      std::cerr << "rocksdb\tmlc_level_" << i << "_hits\t" << hits
                << std::endl;
      std::cerr << "rocksdb\tmlc_level_" << i << "_hit_ratio\t"
                << level_hit_ratio << std::endl;
      // Foreground-only breakdown (excludes compaction-induced lookups) so we
      // can see, per level: (1) what fraction of traffic is compaction, and
      // (2) how the foreground-only hit rate differs from the total hit rate.
      // This is the diagnostic that decides whether excluding compaction from
      // the model can matter at all.
      if (i < snapshot.fg_lookups.size() && i < snapshot.fg_hits.size()) {
        const uint64_t fg_lookups = snapshot.fg_lookups[i];
        const uint64_t fg_hits = snapshot.fg_hits[i];
        total_fg_lookups += fg_lookups;
        total_fg_hits += fg_hits;
        const double fg_hit_ratio =
            fg_lookups > 0
                ? static_cast<double>(fg_hits) / static_cast<double>(fg_lookups)
                : 0.0;
        const uint64_t compaction_lookups =
            lookups >= fg_lookups ? lookups - fg_lookups : 0;
        const double compaction_fraction =
            lookups > 0 ? static_cast<double>(compaction_lookups) /
                              static_cast<double>(lookups)
                        : 0.0;
        std::cerr << "rocksdb\tmlc_level_" << i << "_fg_lookups\t" << fg_lookups
                  << std::endl;
        std::cerr << "rocksdb\tmlc_level_" << i << "_fg_hits\t" << fg_hits
                  << std::endl;
        std::cerr << "rocksdb\tmlc_level_" << i << "_fg_hit_ratio\t"
                  << fg_hit_ratio << std::endl;
        std::cerr << "rocksdb\tmlc_level_" << i << "_compaction_fraction\t"
                  << compaction_fraction << std::endl;
      }
      if (i < snapshot.capacities.size()) {
        std::cerr << "rocksdb\tmlc_level_" << i << "_capacity\t"
                  << snapshot.capacities[i] << std::endl;
      }
      if (i < snapshot.usages.size()) {
        std::cerr << "rocksdb\tmlc_level_" << i << "_usage\t"
                  << snapshot.usages[i] << std::endl;
      }
      // AutoHCC sparse-table Evict diagnostic: table slots (grow-only) vs live
      // entries. A large table_address_count with small occupancy_count means
      // the table stayed big after the allocator shrank this sub-cache, so
      // every insert-time Evict sweeps a sparse oversized table.
      if (i < snapshot.table_address_counts.size() &&
          i < snapshot.occupancy_counts.size()) {
        const size_t table_slots = snapshot.table_address_counts[i];
        const size_t occ = snapshot.occupancy_counts[i];
        const double occ_ratio =
            table_slots > 0
                ? static_cast<double>(occ) / static_cast<double>(table_slots)
                : 0.0;
        std::cerr << "rocksdb\tmlc_level_" << i << "_table_slots\t" << table_slots
                  << std::endl;
        std::cerr << "rocksdb\tmlc_level_" << i << "_occupancy\t" << occ
                  << std::endl;
        std::cerr << "rocksdb\tmlc_level_" << i << "_table_occupancy_ratio\t"
                  << occ_ratio << std::endl;
      }
      if (i < snapshot.data_sizes.size()) {
        std::cerr << "rocksdb\tmlc_level_" << i << "_data_size\t"
                  << snapshot.data_sizes[i] << std::endl;
      }
    }
    std::string stats_text = multi_level_cache_->PrintStats();
    size_t cursor = 0;
    while (cursor < stats_text.size()) {
      size_t line_end = stats_text.find('\n', cursor);
      if (line_end == std::string::npos) {
        line_end = stats_text.size();
      }
      const std::string line = stats_text.substr(cursor, line_end - cursor);
      if (!line.empty() && line[0] == 'L') {
        const size_t level_sep = line.find(':');
        const size_t mode_pos = line.find("probation_insert=");
        if (level_sep != std::string::npos && mode_pos != std::string::npos &&
            level_sep > 1) {
          size_t level = 0;
          try {
            level = static_cast<size_t>(
                std::stoul(line.substr(1, level_sep - 1)));
            const std::string mode_token = line.substr(
                mode_pos + std::strlen("probation_insert="));
            const size_t comma = mode_token.find(',');
            const std::string mode_value =
                comma == std::string::npos ? mode_token
                                           : mode_token.substr(0, comma);
            const int probation = std::stoi(mode_value);
            std::cerr << "rocksdb\tmlc_level_" << level
                      << "_probation_insert\t" << probation << std::endl;
          } catch (...) {
            // Ignore parse errors and keep benchmark running.
          }
        }
      }
      cursor = line_end + 1;
    }
    const double total_level_hit_ratio =
        total_lookups > 0
            ? static_cast<double>(total_hits) / static_cast<double>(total_lookups)
            : 0.0;
    std::cerr << "rocksdb\tmlc_total_lookups\t" << total_lookups << std::endl;
    std::cerr << "rocksdb\tmlc_total_hits\t" << total_hits << std::endl;
    std::cerr << "rocksdb\tmlc_total_hit_ratio\t" << total_level_hit_ratio
              << std::endl;
    // Foreground-only aggregate hit ratio (compaction lookups excluded). Better
    // reflects the cache's value to the application than the total-traffic ratio
    // above, which is diluted by one-shot compaction reads.
    const double total_fg_hit_ratio =
        total_fg_lookups > 0 ? static_cast<double>(total_fg_hits) /
                                   static_cast<double>(total_fg_lookups)
                             : 0.0;
    std::cerr << "rocksdb\tmlc_total_fg_lookups\t" << total_fg_lookups
              << std::endl;
    std::cerr << "rocksdb\tmlc_total_fg_hits\t" << total_fg_hits << std::endl;
    std::cerr << "rocksdb\tmlc_total_fg_hit_ratio\t" << total_fg_hit_ratio
              << std::endl;
  }
}

void RocksdbDB::ResetStats() {
  // Called once in main() at the load->transaction boundary. Drain background
  // compaction here so every scheme measures against an identical, stable LSM.
  // Mirrors db_bench's waitforcompaction step exactly (default
  // WaitForCompactOptions: no flush, no purge; preceded by a 5s sleep so any
  // post-load background work is actually scheduled before we wait). Using
  // flush=true/wait_for_purge=true here deadlocked the close path, so we keep
  // the proven db_bench recipe. Runs for skipload cases too (fast no-op when
  // the DB is already quiescent).
  if (wait_for_compact_before_txn_ && db_ != nullptr) {
    db_->GetEnv()->SleepForMicroseconds(5 * 1000000);
    rocksdb::Status s = db_->WaitForCompact(rocksdb::WaitForCompactOptions());
    if (!s.ok()) {
      std::cerr << "# WaitForCompact before transactions failed: "
                << s.ToString() << std::endl;
    }
  }
  if (statistics_ != nullptr) {
    statistics_->Reset();
  }
  if (multi_level_cache_ != nullptr) {
    multi_level_cache_->ResetStats();
  }
  // Wrapper-policy hit/lookup counters (ARC/Cacheus) live outside the RocksDB
  // statistics object; zero them too so wrapper_hit_ratio only reflects the
  // transaction phase. Name()-based dispatch because RTTI may be disabled.
  if (block_cache_ != nullptr) {
    const char* name = block_cache_->Name();
    if (std::strcmp(name, rocksdb::ARCCache::kClassName()) == 0) {
      static_cast<rocksdb::ARCCache*>(block_cache_.get())
          ->ResetWrapperCounters();
    } else if (std::strcmp(name, rocksdb::CacheusCache::kClassName()) == 0) {
      static_cast<rocksdb::CacheusCache*>(block_cache_.get())
          ->ResetWrapperCounters();
    } else if (std::strcmp(name, rocksdb::ShardedWrapperCache::kClassName()) ==
               0) {
      static_cast<rocksdb::ShardedWrapperCache*>(block_cache_.get())
          ->ResetWrapperCounters();
    }
  }
}

int RocksdbDB::Read(const std::string& table, const std::string& key,
                    const std::vector<std::string>* fields,
                    std::vector<KVPair>& result) {
  result.clear();
  if (!db_) {
    return DB::kErrorConflict;
  }

  std::string value;
  const rocksdb::Status s =
      db_->Get(rocksdb::ReadOptions(),
               BuildInternalKey(table, key, raw_kv_mode_, raw_key_size_bytes_),
               &value);
  if (s.IsNotFound()) {
    return DB::kErrorNoData;
  }
  if (!s.ok()) {
    return DB::kErrorConflict;
  }

  if (raw_kv_mode_) {
    result.emplace_back("field0", value);
    return DB::kOK;
  }

  std::vector<KVPair> decoded;
  if (!DecodeRecord(value, &decoded)) {
    return DB::kErrorConflict;
  }
  SelectFields(decoded, fields, &result);
  return DB::kOK;
}

int RocksdbDB::Scan(const std::string& table, const std::string& key,
                    int record_count, const std::vector<std::string>* fields,
                    std::vector<std::vector<KVPair>>& result) {
  result.clear();
  if (!db_) {
    return DB::kErrorConflict;
  }

  // Whole-scan latency (Seek + all Nexts + decode), recorded into the
  // DB_MULTIGET histogram slot: this workload never calls MultiGet, so the
  // slot is otherwise empty, and reusing it inherits the existing
  // stats-reset / percentile-export plumbing (emitted as lat_scan_*).
  // RocksDB's own DB_SEEK histogram only times the Seek call, which is a
  // small fraction of a multi-record scan's cost.
  const auto scan_start = std::chrono::steady_clock::now();

  const std::string table_prefix = table;
  const std::string start_key =
      BuildInternalKey(table, key, raw_kv_mode_, raw_key_size_bytes_);
  rocksdb::ReadOptions ro;
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(ro));

  iter->Seek(start_key);
  while (iter->Valid() && record_count > 0) {
    const std::string internal_key = iter->key().ToString();
    if (!raw_kv_mode_ && !StartsWith(internal_key, table_prefix)) {
      break;
    }
    if (raw_kv_mode_) {
      std::vector<KVPair> selected;
      selected.emplace_back("field0", iter->value().ToString());
      result.emplace_back(std::move(selected));
      --record_count;
      iter->Next();
      continue;
    }
    std::vector<KVPair> decoded;
    if (!DecodeRecord(iter->value().ToString(), &decoded)) {
      return DB::kErrorConflict;
    }
    std::vector<KVPair> selected;
    SelectFields(decoded, fields, &selected);
    result.emplace_back(std::move(selected));
    --record_count;
    iter->Next();
  }

  if (statistics_) {
    const uint64_t micros =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - scan_start)
            .count();
    statistics_->recordInHistogram(rocksdb::DB_MULTIGET, micros);
  }
  if (!iter->status().ok()) {
    return DB::kErrorConflict;
  }
  return DB::kOK;
}

int RocksdbDB::Update(const std::string& table, const std::string& key,
                      std::vector<KVPair>& values) {
  if (!db_) {
    return DB::kErrorConflict;
  }

  const std::string internal_key =
      BuildInternalKey(table, key, raw_kv_mode_, raw_key_size_bytes_);
  std::string existing;
  std::vector<KVPair> merged;

  const rocksdb::Status get_s = db_->Get(rocksdb::ReadOptions(), internal_key, &existing);
  if (get_s.ok()) {
    if (raw_kv_mode_) {
      rocksdb::WriteOptions wo;
      wo.sync = sync_writes_;
      const rocksdb::Status put_s = db_->Put(wo, internal_key, raw_value_template_);
      return put_s.ok() ? DB::kOK : DB::kErrorConflict;
    } else {
      if (!DecodeRecord(existing, &merged)) {
        return DB::kErrorConflict;
      }
    }
  } else if (!get_s.IsNotFound()) {
    return DB::kErrorConflict;
  }

  std::unordered_map<std::string, size_t> field_pos;
  field_pos.reserve(merged.size() + values.size());
  for (size_t i = 0; i < merged.size(); ++i) {
    field_pos[merged[i].first] = i;
  }
  for (const auto& kv : values) {
    auto it = field_pos.find(kv.first);
    if (it == field_pos.end()) {
      field_pos[kv.first] = merged.size();
      merged.push_back(kv);
    } else {
      merged[it->second].second = kv.second;
    }
  }

  rocksdb::WriteOptions wo;
  wo.sync = sync_writes_;
  const rocksdb::Status put_s =
      db_->Put(wo, internal_key, EncodeRecord(merged));
  return put_s.ok() ? DB::kOK : DB::kErrorConflict;
}

int RocksdbDB::Insert(const std::string& table, const std::string& key,
                      std::vector<KVPair>& values) {
  if (!db_) {
    return DB::kErrorConflict;
  }

  const std::string internal_key =
      BuildInternalKey(table, key, raw_kv_mode_, raw_key_size_bytes_);
  std::string existing;
  const rocksdb::Status get_s =
      db_->Get(rocksdb::ReadOptions(), internal_key, &existing);
  if (get_s.ok()) {
    ++insert_get_ok_count_;
    return DB::kErrorConflict;
  }
  if (get_s.IsNotFound()) {
    ++insert_get_not_found_count_;
  } else {
    ++insert_get_other_count_;
    last_insert_get_other_status_ = get_s.ToString();
    return DB::kErrorConflict;
  }

  rocksdb::WriteOptions wo;
  wo.sync = sync_writes_;
  const rocksdb::Status put_s = db_->Put(
      wo, internal_key, raw_kv_mode_ ? raw_value_template_ : EncodeRecord(values));
  if (put_s.ok()) {
    ++insert_put_ok_count_;
    return DB::kOK;
  }
  ++insert_put_fail_count_;
  last_insert_put_fail_status_ = put_s.ToString();
  return DB::kErrorConflict;
}

int RocksdbDB::Delete(const std::string& table, const std::string& key) {
  if (!db_) {
    return DB::kErrorConflict;
  }

  const std::string internal_key =
      BuildInternalKey(table, key, raw_kv_mode_, raw_key_size_bytes_);
  std::string existing;
  const rocksdb::Status get_s =
      db_->Get(rocksdb::ReadOptions(), internal_key, &existing);
  if (get_s.IsNotFound()) {
    return DB::kErrorNoData;
  }
  if (!get_s.ok()) {
    return DB::kErrorConflict;
  }

  rocksdb::WriteOptions wo;
  wo.sync = sync_writes_;
  const rocksdb::Status del_s = db_->Delete(wo, internal_key);
  return del_s.ok() ? DB::kOK : DB::kErrorConflict;
}

void RocksdbDB::AppendUint32(std::string* dst, uint32_t value) {
  dst->push_back(static_cast<char>(value & 0xFF));
  dst->push_back(static_cast<char>((value >> 8) & 0xFF));
  dst->push_back(static_cast<char>((value >> 16) & 0xFF));
  dst->push_back(static_cast<char>((value >> 24) & 0xFF));
}

bool RocksdbDB::ReadUint32(const std::string& src, size_t* pos, uint32_t* value) {
  if (*pos + 4 > src.size()) {
    return false;
  }
  const unsigned char b0 = static_cast<unsigned char>(src[*pos]);
  const unsigned char b1 = static_cast<unsigned char>(src[*pos + 1]);
  const unsigned char b2 = static_cast<unsigned char>(src[*pos + 2]);
  const unsigned char b3 = static_cast<unsigned char>(src[*pos + 3]);
  *value = static_cast<uint32_t>(b0) | (static_cast<uint32_t>(b1) << 8) |
           (static_cast<uint32_t>(b2) << 16) |
           (static_cast<uint32_t>(b3) << 24);
  *pos += 4;
  return true;
}

std::string RocksdbDB::EncodeRecord(const std::vector<KVPair>& fields) {
  std::string encoded;
  encoded.reserve(fields.size() * 32);
  AppendUint32(&encoded, static_cast<uint32_t>(fields.size()));
  for (const auto& kv : fields) {
    AppendUint32(&encoded, static_cast<uint32_t>(kv.first.size()));
    encoded.append(kv.first);
    AppendUint32(&encoded, static_cast<uint32_t>(kv.second.size()));
    encoded.append(kv.second);
  }
  return encoded;
}

bool RocksdbDB::DecodeRecord(const std::string& encoded,
                             std::vector<KVPair>* fields) {
  fields->clear();
  size_t pos = 0;
  uint32_t count = 0;
  if (!ReadUint32(encoded, &pos, &count)) {
    return false;
  }
  fields->reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    uint32_t key_len = 0;
    uint32_t val_len = 0;
    if (!ReadUint32(encoded, &pos, &key_len)) {
      return false;
    }
    if (pos + key_len > encoded.size()) {
      return false;
    }
    std::string field = encoded.substr(pos, key_len);
    pos += key_len;
    if (!ReadUint32(encoded, &pos, &val_len)) {
      return false;
    }
    if (pos + val_len > encoded.size()) {
      return false;
    }
    std::string value = encoded.substr(pos, val_len);
    pos += val_len;
    fields->emplace_back(std::move(field), std::move(value));
  }
  return pos == encoded.size();
}

std::string RocksdbDB::BuildInternalKey(const std::string& table,
                                        const std::string& key,
                                        bool raw_kv_mode,
                                        size_t raw_key_size_bytes) {
  if (!raw_kv_mode) {
    return table + key;
  }
  if (key.size() == raw_key_size_bytes) {
    return key;
  }
  if (key.size() > raw_key_size_bytes) {
    return key.substr(key.size() - raw_key_size_bytes);
  }
  return std::string(raw_key_size_bytes - key.size(), '0') + key;
}

bool RocksdbDB::StartsWith(const std::string& value, const std::string& prefix) {
  if (value.size() < prefix.size()) {
    return false;
  }
  return value.compare(0, prefix.size(), prefix) == 0;
}

void RocksdbDB::SelectFields(const std::vector<KVPair>& all_fields,
                             const std::vector<std::string>* fields,
                             std::vector<KVPair>* selected) {
  selected->clear();
  if (fields == nullptr) {
    *selected = all_fields;
    return;
  }
  selected->reserve(fields->size());
  for (const auto& wanted : *fields) {
    for (const auto& kv : all_fields) {
      if (kv.first == wanted) {
        selected->push_back(kv);
        break;
      }
    }
  }
}

}  // namespace ycsbc

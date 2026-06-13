#include "db/rocksdb_db.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
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

  if (cache_type == "lru_cache") {
    rocksdb::LRUCacheOptions lru_opts(
        cache_capacity, cache_numshardbits, strict_capacity_limit,
        high_pri_pool_ratio, nullptr, rocksdb::kDefaultToAdaptiveMutex,
        rocksdb::kDefaultCacheMetadataChargePolicy, low_pri_pool_ratio);
    lru_opts.hash_seed = cache_hash_seed;
    auto cache = std::make_shared<rocksdb::MultiLevelCache>(
        static_cast<size_t>(num_levels), cache_capacity, lru_opts, force_l0);
    cache->SetSharedPoolRatio(
        ParseDouble(props, "rocksdb.multi_level_cache_shared_pool_ratio", 0.0));
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
    cache->SetSharedPoolRatio(
        ParseDouble(props, "rocksdb.multi_level_cache_shared_pool_ratio", 0.0));
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
    cache->SetSharedPoolRatio(
        ParseDouble(props, "rocksdb.multi_level_cache_shared_pool_ratio", 0.0));
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

    const int srhcc_start_level =
        ParseInt(props, "rocksdb.multi_level_cache_srhcc_start_level", -1);
    const bool mixed_srhcc = srhcc_start_level >= 0 &&
                             srhcc_start_level < num_levels;
    if (!mixed_srhcc) {
      auto cache = std::make_shared<rocksdb::MultiLevelCache>(
          static_cast<size_t>(num_levels), cache_capacity, hcc_opts, force_l0);
      cache->SetSharedPoolRatio(
          ParseDouble(props, "rocksdb.multi_level_cache_shared_pool_ratio", 0.0));
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
      if (level >= static_cast<size_t>(srhcc_start_level)) {
        per_level.probation_insert = true;
      }
      auto sub = per_level.MakeSharedCache();
      sub->SetCapacity(level_capacity);
      sub_caches.emplace_back(std::move(sub));
    }
    rocksdb::HyperClockCacheOptions shared_opts = hcc_opts;
    shared_opts.capacity = std::max<size_t>(1, cache_capacity);
    auto shared_cache = shared_opts.MakeSharedCache();
    shared_cache->SetCapacity(0);
    auto cache = std::make_shared<rocksdb::MultiLevelCache>(
        std::move(sub_caches), std::move(shared_cache), cache_capacity);
    cache->SetSharedPoolRatio(
        ParseDouble(props, "rocksdb.multi_level_cache_shared_pool_ratio", 0.0));
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
  options.compression = ParseCompressionType(
      props.GetProperty("rocksdb.compression", "none"));
  options.use_direct_reads = ParseBool(props, "rocksdb.use_direct_reads", false);
  options.use_direct_io_for_flush_and_compaction = ParseBool(
      props, "rocksdb.use_direct_io_for_flush_and_compaction", false);
  options.allow_mmap_reads = ParseBool(props, "rocksdb.allow_mmap_reads", false);
  options.statistics = rocksdb::CreateDBStatistics();
  statistics_ = options.statistics;

  if (ParseBool(props, "rocksdb.optimize_level_style_compaction", true)) {
    options.OptimizeLevelStyleCompaction();
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
                             1000)));
    alloc_opts.smoothing_ratio =
        ParseDouble(props, "rocksdb.multi_level_cache_adjust_smoothing_ratio",
                    0.5);
    alloc_opts.min_total_change_bytes = static_cast<size_t>(ParseUint64(
        props, "rocksdb.multi_level_cache_adjust_min_change_bytes", 1ULL << 20));
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
    const size_t alpha_window_rounds = static_cast<size_t>(std::max(
        1, ParseInt(props, "rocksdb.multi_level_cache_alpha_window_rounds", 5)));
    const size_t data_window_rounds = static_cast<size_t>(std::max(
        1,
        ParseInt(props, "rocksdb.multi_level_cache_data_size_window_rounds", 5)));
    const uint64_t data_window_us =
        static_cast<uint64_t>(std::max(
            1, ParseInt(props, "rocksdb.multi_level_cache_data_size_window_ms",
                        5000))) *
        1000;
    const bool use_constant_one_alpha = alpha_estimator != "robust_hit_rate";

    multi_level_allocator_ = std::make_unique<rocksdb::MultiLevelCacheAllocator>(
        multi_level_cache_,
        [cache = multi_level_cache_, alpha_floor, alpha_max,
         use_effective_data_size, alpha_window_rounds, data_window_rounds,
         data_window_us, use_constant_one_alpha,
         prev_lookups = std::vector<uint64_t>{},
         prev_hits = std::vector<uint64_t>{},
         prev_alpha = std::vector<double>{},
         prev_effective_data = std::vector<double>{},
         observed_history =
             std::vector<std::deque<std::pair<uint64_t, uint64_t>>>{},
         raw_data_history =
             std::vector<std::deque<std::pair<uint64_t, double>>>{}](
            std::vector<double>* lambda, std::vector<double>* data,
            std::vector<double>* alpha) mutable {
          if (cache == nullptr || lambda == nullptr || data == nullptr ||
              alpha == nullptr) {
            return false;
          }
          const auto stats = cache->GetLevelMetricsSnapshot();
          const size_t level_count = stats.lookups.size();
          if (level_count == 0 || stats.hits.size() != level_count ||
              stats.capacities.size() != level_count ||
              stats.data_sizes.size() != level_count) {
            return false;
          }
          if (prev_lookups.size() != level_count ||
              prev_hits.size() != level_count ||
              prev_alpha.size() != level_count ||
              prev_effective_data.size() != level_count ||
              observed_history.size() != level_count ||
              raw_data_history.size() != level_count) {
            prev_lookups = stats.lookups;
            prev_hits = stats.hits;
            prev_alpha.assign(level_count, 1.0);
            prev_effective_data.assign(level_count, 1.0);
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

          uint64_t total_observed_data = 0;
          size_t observed_levels = 0;
          for (size_t level = 0; level < level_count; ++level) {
            if (stats.data_sizes[level] > 0) {
              total_observed_data += stats.data_sizes[level];
              ++observed_levels;
            }
          }
          const double default_data =
              observed_levels > 0
                  ? static_cast<double>(total_observed_data) /
                        static_cast<double>(observed_levels)
                  : 1.0;
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
                curr_hits >= prev_hits[level] ? curr_hits - prev_hits[level]
                                              : 0;

            (*lambda)[level] = delta_lookups > 0
                                   ? static_cast<double>(delta_lookups)
                                   : kLambdaEpsilon;
            const double raw_level_data_instant =
                stats.data_sizes[level] > 0
                    ? static_cast<double>(stats.data_sizes[level])
                    : default_data;
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
            if (use_constant_one_alpha) {
              (*alpha)[level] = 1.0;
              (*data)[level] = raw_level_data;
              prev_alpha[level] = 1.0;
              prev_effective_data[level] = raw_level_data;
              continue;
            }

            // Robust online alpha estimation:
            //   1) derive raw alpha from observed window hit rate
            //   2) confidence-shrink to prior under small samples
            //   3) EMA smooth across windows
            double derived_alpha = prev_alpha[level];
            const size_t capacity_bytes = stats.capacities[level];
            double observed_hit_rate = -1.0;
            if (delta_lookups > 0 && capacity_bytes > 0 &&
                raw_level_data > 0.0) {
              observed_history[level].push_back({delta_hits, delta_lookups});
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
                  -(raw_level_data / static_cast<double>(capacity_bytes)) *
                  std::log(miss_rate);
            }

            const double confidence =
                std::min(1.0, static_cast<double>(delta_lookups) /
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
                const double data_confidence = std::min(
                    1.0, static_cast<double>(delta_lookups) /
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
            (*data)[level] =
                use_effective_data_size ? effective_level_data : raw_level_data;
            prev_effective_data[level] = effective_level_data;
          }
          prev_lookups = stats.lookups;
          prev_hits = stats.hits;
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
      if (i < snapshot.capacities.size()) {
        std::cerr << "rocksdb\tmlc_level_" << i << "_capacity\t"
                  << snapshot.capacities[i] << std::endl;
      }
      if (i < snapshot.usages.size()) {
        std::cerr << "rocksdb\tmlc_level_" << i << "_usage\t"
                  << snapshot.usages[i] << std::endl;
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
  }
}

void RocksdbDB::ResetStats() {
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

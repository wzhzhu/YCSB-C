#ifndef YCSB_C_ROCKSDB_DB_H_
#define YCSB_C_ROCKSDB_DB_H_

#include <memory>
#include <string>
#include <vector>

#include "core/db.h"
#include "core/properties.h"

namespace rocksdb {
class DB;
class Cache;
class MultiLevelCache;
class MultiLevelCacheAllocator;
class Statistics;
}

namespace ycsbc {

class RocksdbDB : public DB {
 public:
  explicit RocksdbDB(const utils::Properties& props);
  ~RocksdbDB() override;

  int Read(const std::string& table, const std::string& key,
           const std::vector<std::string>* fields,
           std::vector<KVPair>& result) override;

  int Scan(const std::string& table, const std::string& key, int record_count,
           const std::vector<std::string>* fields,
           std::vector<std::vector<KVPair>>& result) override;

  int Update(const std::string& table, const std::string& key,
             std::vector<KVPair>& values) override;

  int Insert(const std::string& table, const std::string& key,
             std::vector<KVPair>& values) override;

  int Delete(const std::string& table, const std::string& key) override;
  void ResetStats() override;

 private:
  static void AppendUint32(std::string* dst, uint32_t value);
  static bool ReadUint32(const std::string& src, size_t* pos, uint32_t* value);
  static std::string EncodeRecord(const std::vector<KVPair>& fields);
  static bool DecodeRecord(const std::string& encoded,
                           std::vector<KVPair>* fields);
  static std::string BuildInternalKey(const std::string& table,
                                      const std::string& key,
                                      bool raw_kv_mode,
                                      size_t raw_key_size_bytes);
  static bool StartsWith(const std::string& value, const std::string& prefix);
  static void SelectFields(const std::vector<KVPair>& all_fields,
                           const std::vector<std::string>* fields,
                           std::vector<KVPair>* selected);

  std::unique_ptr<rocksdb::DB> db_;
  std::shared_ptr<rocksdb::Cache> block_cache_;
  std::shared_ptr<rocksdb::MultiLevelCache> multi_level_cache_;
  std::unique_ptr<rocksdb::MultiLevelCacheAllocator> multi_level_allocator_;
  std::vector<uint64_t> prev_allocator_lookups_;
  std::vector<uint64_t> prev_allocator_hits_;
  std::shared_ptr<rocksdb::Statistics> statistics_;
  bool raw_kv_mode_ = false;
  size_t raw_key_size_bytes_ = 24;
  size_t raw_value_size_bytes_ = 1000;
  std::string raw_value_template_;
  uint64_t insert_get_ok_count_ = 0;
  uint64_t insert_get_not_found_count_ = 0;
  uint64_t insert_get_other_count_ = 0;
  uint64_t insert_put_ok_count_ = 0;
  uint64_t insert_put_fail_count_ = 0;
  std::string last_insert_get_other_status_;
  std::string last_insert_put_fail_status_;
  std::string db_path_;
  bool sync_writes_ = false;
};

}  // namespace ycsbc

#endif  // YCSB_C_ROCKSDB_DB_H_

//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include <cstdint>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

struct ThreadRunStats {
  uint64_t ok_ops = 0;
  uint64_t read_ops = 0;
  uint64_t write_ops = 0;
};

int OpsForThread(int total_ops, int thread_id, int thread_count) {
  if (thread_count <= 0) {
    return 0;
  }
  const int base = total_ops / thread_count;
  const int remainder = total_ops % thread_count;
  return base + (thread_id < remainder ? 1 : 0);
}

ThreadRunStats DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl,
                              const int num_ops, bool is_loading) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  ThreadRunStats stats;
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      stats.ok_ops += client.DoInsert();
      continue;
    }

    ycsbc::Operation op = ycsbc::READ;
    const bool ok = client.DoTransaction(&op);
    stats.ok_ops += ok;
    if (op == ycsbc::READ || op == ycsbc::SCAN) {
      ++stats.read_ops;
    } else {
      ++stats.write_ops;
    }
  }
  db->Close();
  return stats;
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);
  bool skip_load = false;
  try {
    skip_load = utils::StrToBool(props.GetProperty("skipload", "false"));
  } catch (...) {
    skip_load = false;
  }

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  const int load_threads = stoi(
      props.GetProperty("loadthreadcount", props.GetProperty("threadcount", "1")));

  // Loads data
  vector<future<ThreadRunStats>> actual_ops;
  uint64_t sum = 0;
  int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
  if (!skip_load) {
    for (int i = 0; i < load_threads; ++i) {
      const int ops_for_thread = OpsForThread(total_ops, i, load_threads);
      actual_ops.emplace_back(async(launch::async,
          DelegateClient, db, &wl, ops_for_thread, true));
    }
    assert((int)actual_ops.size() == load_threads);

    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get().ok_ops;
    }
  }
  cerr << "# Loading records:\t" << sum << endl;
  db->ResetStats();

  // Peforms transactions
  actual_ops.clear();
  total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
  utils::Timer<double> timer;
  timer.Start();
  uint64_t executed_ops = 0;
  uint64_t read_ops = 0;
  uint64_t write_ops = 0;
  for (int i = 0; i < num_threads; ++i) {
    const int ops_for_thread = OpsForThread(total_ops, i, num_threads);
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, &wl, ops_for_thread, false));
  }
  assert((int)actual_ops.size() == num_threads);

  for (auto &n : actual_ops) {
    assert(n.valid());
    ThreadRunStats stats = n.get();
    executed_ops += stats.ok_ops;
    read_ops += stats.read_ops;
    write_ops += stats.write_ops;
  }
  double duration = timer.End();
  const double throughput_kops = duration > 0 ? executed_ops / duration / 1000.0 : 0;
  const double read_throughput_kops = duration > 0 ? read_ops / duration / 1000.0 : 0;
  const double write_throughput_kops = duration > 0 ? write_ops / duration / 1000.0 : 0;
  cerr << "# Transaction throughput (KTPS)" << endl;
  cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
  cerr << throughput_kops << endl;
  cerr << "# Transaction op breakdown" << endl;
  cerr << "rocksdb\texecuted_ops\t" << executed_ops << endl;
  cerr << "rocksdb\tread_ops\t" << read_ops << endl;
  cerr << "rocksdb\twrite_ops\t" << write_ops << endl;
  cerr << "rocksdb\tread_throughput_kops\t" << read_throughput_kops << endl;
  cerr << "rocksdb\twrite_throughput_kops\t" << write_throughput_kops << endl;
  delete db;
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-load-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("loadthreadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-skipload") == 0 ||
               strcmp(argv[argindex], "--skip-load") == 0) {
      props.SetProperty("skipload", "true");
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -load-threads n: execute load phase using n threads "
       << "(default: same as -threads)" << endl;
  cout << "  -db dbname: specify DB backend (basic | lock_stl | tbb_rand | "
       << "tbb_scan | redis | rocksdb), default: basic" << endl;
  cout << "  -skipload | --skip-load: skip load phase and run transactions only" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}


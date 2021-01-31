#include <iostream>
#include <cmath>
#include <sys/time.h>
#include "emu_environment.h"
#include <algorithm>



/*Set up the singleton object with the experiment wide options*/
EmuEnv* EmuEnv::instance = 0;


EmuEnv::EmuEnv() 
{

  delete_persistence_latency = 2250; // in secs
  level_delete_persistence_latency = new double[20]; // in secs
  RR_level_last_file_selected = new int[20]; // !YBS-sep06-XX!

  flag = 0;
  oldest_delete_file_timestamp = std::chrono::system_clock::now();

  // First-Entry Flags (FEFs) 
  version_set_FEF = false; // !YBS-sep06-XX!
  
  destroy_database = 1;
  clear_system_cache = 1; // !YBS-sep09-XX!
  show_progress = 0; // !YBS-sep17-XX!
  current_op = ' '; // !YBS-sep18-XX!

// Options set through command line 
  size_ratio = 10;
  buffer_size_in_pages = 4096;
  entries_per_page = 4;
  entry_size = 1024; // in Bytes 
  buffer_size = buffer_size_in_pages * entries_per_page * entry_size; // M = P*B*E = 128 * 128 * 128 B = 2 MB 
  file_to_memtable_size_ratio = 1; // f
  file_size = buffer_size * file_to_memtable_size_ratio; // F
  verbosity = 0;

  // adding new parameters with Guanting
  compaction_pri = 1; // c // 1 for kMinOverlappingRatio, 2 for kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for kOldestSmallestSeqFirst, 5 for kEffacingCompaction
  bits_per_key = 10; // b


// Options hardcoded in code
  // Memory allocation options
    max_write_buffer_number = 2;
    memtable_factory = 3; // 1 for skiplist, 2 for vector, 3 for hash skiplist, 4 for hash linklist
    target_file_size_base = buffer_size;
    level_compaction_dynamic_level_bytes = false;
    compaction_style = 1; // 1 for kCompactionStyleLevel, 2 for kCompactionStyleUniversal, 3 for kCompactionStyleFIFO, 4 for kCompactionStyleNone 
    disable_auto_compactions = false; // TBC
    compaction_filter = 0; // 0 for nullptr, 1 for invoking custom compaction filter, if any
    compaction_filter_factory = 0; // 0 for nullptr, 1 for invoking custom compaction filter factory, if any
    access_hint_on_compaction_start = 2; // TBC
    level0_file_num_compaction_trigger = 1; // TBC
    target_file_size_multiplier = 1; // TBC
    max_background_jobs = 1; // TBC
    max_compaction_bytes = 0; // TBC
    max_bytes_for_level_base = buffer_size * size_ratio; // TBC
    merge_operator = 0; 
    soft_pending_compaction_bytes_limit = 0;    // No pending compaction anytime, try and see
    hard_pending_compaction_bytes_limit = 0;    // No pending compaction anytime, try and see
    periodic_compaction_seconds = 0;
    use_direct_io_for_flush_and_compaction = true;
    live_levels = 0; //!YBS-sep07-XX!
    num_levels = 10; // Maximum number of levels that a tree may have [RDB_default: 7]

  // TableOptions
    no_block_cache = false; // TBC
    block_cache = 0; // in MB //!YBS-sep09-XX!
    block_cache_high_priority_ratio = 0.5; //!YBS-sep09-XX!
    cache_index_and_filter_blocks = true;
    cache_index_and_filter_blocks_with_high_priority = true;    // Deprecated by no_block_cache
    read_amp_bytes_per_bit = 4; // temporarily 4; why 4 ?
    data_block_index_type = 1; // 1 for kDataBlockBinarySearch, 2 for kDataBlockBinaryAndHash
    index_type = 1; // 1 for kBinarySearch, 2 for kHashSearch, 3 for kTwoLevelIndexSearch, 4 for kBinarySearchWithFirstKey
    partition_filters = false;
    metadata_block_size = 4096;   // currently deprecated by data_block_index_type; TBC
    pin_top_level_index_and_filter = false; // TBC
    index_shortening = 1; // 1 for kNoShortening, 2 for kShortenSeparators, 3 for kShortenSeparatorsAndSuccessor
    block_size_deviation = 0; // TBC
    enable_index_compression = false; // TBC

  //Compression
    compression = 1; // 1 for kNoCompression, 2 for kSnappyCompression, 3 for kZlibCompression, 4 for kBZip2Compression, 5 for kLZ4Compression, 6 for kLZ4HCCompression, 7 for kXpressCompression, 8 for kZSTD, 9 for kZSTDNotFinalCompression, 10 for kDisableCompressionOption

  // ReadOptions
    verify_checksums = true; // TBC
    fill_cache = false; // data block/index block read for this iteration will not be cached
    iter_start_seqnum = 0; // TBC
    ignore_range_deletions = false; // TBC
    read_tier = 1; // 1 for kReadAllTier, 2 for kBlockCacheTier, 3 for kPersistedTier, 4 for kMemtableTier

  // WriteOptions
    low_pri = true; // every insert is less important than compaction
    sync = false; // make every write wait for sync with log (so we see real perf impact of insert)
    disableWAL = false; // TBC
    no_slowdown = false; // enabling this will make some insertions fail 
    ignore_missing_column_families = false; // TBC

  // Other CFOptions
    comparator = 1; // 1 for BytewiseComparator(), 2 for ...
    max_sequential_skip_in_iterations = 8; // TBC
    memtable_prefix_bloom_size_ratio = 0;    // disabled
    level0_stop_writes_trigger = 2; // need to try with 1 ; TBC
    paranoid_file_checks = false;
    optimize_filters_for_hits = false;
    inplace_update_support = false;
    inplace_update_num_locks = 10000;
    report_bg_io_stats = true;
    max_successive_merges = 0;   // read-modified-write related

  // Other DBOptions
    create_if_missing = true;
    delayed_write_rate = 0;
    max_open_files = 99999;    // resetting to 20
    max_file_opening_threads = 80;
    bytes_per_sync = 0;
    stats_persist_period_sec = 600;
    enable_thread_tracking = false;
    stats_history_buffer_size = 1024 * 1024;
    allow_concurrent_memtable_write = false;
    dump_malloc_stats = false;
    use_direct_reads = true;
    avoid_flush_during_shutdown = false;
    advise_random_on_open = true;
    delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;   // 6 hours
    allow_mmap_reads = false;
    allow_mmap_writes = false;

    // Flush Options
    wait = true;
    allow_write_stall = true;



  // Workload options -- not sure if necessary to have these here!
    int num_inserts = 0;

// old options


  path = ""; 
  debugging = false; 
  FPR_optimization_level = 1; 
  derived_num_levels=-1; 
  N=-1; 
  derived_N=-1; 
  K=1; 
  Z=1; 
  use_block_based_filter=false;
  string experiment_name = ""; 
  string experiment_starting_time = "";
  max_levels=1000;
  measure_IOs=false; 
  total_IOs=0;
  target_level_for_non_zero_result_point_lookups = num_levels;
  key_prefix_for_entries_to_target_in_queries = "+";
  clean_caches_for_experiments=false;
  print_IOs_per_file=false;

  
  file_system_page_size=4096;
  only_tune=false;
  num_read_query_sessions=1;
}

EmuEnv* EmuEnv::getInstance()
{
  if (instance == 0)
    instance = new EmuEnv();

  return instance;
}

// !YBS-sep06-XX
void EmuEnv::AddNewLevel(int _level_count, EmuEnv* _env) {
  _env->live_levels = _level_count;
  if (_env->compaction_pri == 5) {
    EmuEnv::ReSetLevelDeletePersistenceLatency(_level_count, _env);
  }
  else  std::cout << "                                                                                               " << std::endl; 
} 
// !END

void EmuEnv::ReSetLevelDeletePersistenceLatency(int _level_count, EmuEnv* _env) { // reset dpl-per-level when there in a new level added
    // note: last level does not have a del_per_lat (therefore, level id replaced by level-1)
    // !YBS-sep06-XX
      double x = _env->delete_persistence_latency * (_env->size_ratio - 1) / ( pow( _env->size_ratio, (_level_count - 1 ) ) - 1 );
      std::cout << " [ DPL(s) = " << _env->delete_persistence_latency << " = ";
      for (int i = 0; i < _level_count - 1; ++i) { // i=0 corresponds to level-1
        _env->level_delete_persistence_latency[i] = x * pow( _env->size_ratio, i );
        std::cout << _env->level_delete_persistence_latency[i] << " (L" << i << ") + ";
      }
      std::cout << "\b\b]";
      if (_env->show_progress) 
        std::cout << "                                                           " << std::endl;
    // !END
  }

double EmuEnv::GetLevelDeletePersistenceLatency(int _level, EmuEnv* _env){
  return _env->level_delete_persistence_latency[_level]; // index:_level-1 corresponds to level:_level-1
}

void EmuEnv::DumpDeleteFileTimestamp(std::chrono::time_point<std::chrono::system_clock> delete_file_timestamp, uint64_t delete_file_id, EmuEnv* _env){
  std::chrono::time_point<std::chrono::system_clock> current_timestamp = std::chrono::system_clock::now();
  auto current_age = (std::chrono::duration<double, std::milli>(current_timestamp - current_timestamp)).count();
  auto delete_file_age = (std::chrono::duration<double, std::milli>(current_timestamp - delete_file_timestamp)).count();
  if (_env->verbosity >= 3)
    std::cout << "current_age = " << current_age
            << " & delete_file_age = " << delete_file_age << " & delete_file_no = " << delete_file_id; // << std::endl;
  if (delete_file_age > current_age) {
    _env->oldest_delete_file_timestamp = delete_file_timestamp;
  }
  _env->flag+=2;
  if (_env->verbosity >= 3)
    std::cout << " & flag = " << _env->flag << std::endl;
}

std::chrono::time_point<std::chrono::system_clock> EmuEnv::GetDumpedDeleteFileTimestamp(){
  EmuEnv* _env = EmuEnv::getInstance();
  // std::cout << "returning timestamp = " << std::chrono::system_clock::to_time_t(_env->oldest_delete_file_timestamp) << std::endl;
  std::chrono::time_point<std::chrono::system_clock> temp = _env->oldest_delete_file_timestamp;
  --_env->flag;
  if (_env->flag == 0)
    _env->oldest_delete_file_timestamp = std::chrono::system_clock::now();
  if (_env->flag < 0)
    _env->flag =0;
  // std::cout << " reducing flag : flag = " << _env->flag << std::endl;
  return temp;
}

void EmuEnv::PopulatingVector(uint64_t _file_id){
  EmuEnv* _env = EmuEnv::getInstance();
  if (_file_id > 0) {
    // for (int i = 0; i < _env->vec.size(); ++i) 
    //   std::cout << "printing vector state BEFORE: " << _env->vec[i] << "\t";
    // std::cout << "\n";

    _env->vec.push_back(_file_id);
    // std::cout << "pushing file_id = " << _file_id << std::endl;

    // for (int i = 0; i < _env->vec.size(); ++i) 
    //   std::cout << "printing vector state AFTER : " << _env->vec[i] << "\t";
    // std::cout << "\n";
  }
}

int EmuEnv::CheckingVector(uint64_t _file_id){
  EmuEnv* _env = EmuEnv::getInstance();
  bool match = false;
  // for (int i = 0; i < _env->vec.size(); ++i) 
  //   std::cout << "printing vector state BEFORE: " << _env->vec[i] << "\t";
  // std::cout << "\n";

  for (int i = 0; i < _env->vec.size(); ++i) {
    if (_env->vec[i] == _file_id) {
      // std::cout << "MATCH FOUND file_id = " << _file_id << std::endl;
      _env->vec.erase(std::remove(_env->vec.begin(), _env->vec.end(), _file_id), _env->vec.end());
      match = true;
    }
  }
  // for (int i = 0; i < _env->vec.size(); ++i) 
  //   std::cout << "printing vector state AFTER : " << _env->vec[i] << "\t";
  // std::cout << "\n";

  if(match) return 2; return 1;

}

// !YBS-sep06-XX
void EmuEnv::PrintRRIndices(EmuEnv* _env) {
  std::cout << "RRIndexArray = [ ";
  for (int i=0; i<20; i++) {
    std::cout << _env->RR_level_last_file_selected[i] << " ";
  }
  std::cout << " ]" << std::endl;
}
// !END




/*
std::string get_time_string() {
   time_t rawtime;
   struct tm * timeinfo;
   char buffer[80];
   time (&rawtime);
   timeinfo = localtime(&rawtime);
   strftime(buffer,80,"%Y-%m-%d_%H:%M:%S",timeinfo);
   std::string str(buffer);
   return str;
}

bool is_zero(double val)
{
    double epsilon = 0.00001;
    return ( abs(val)<epsilon );
}

// Collect some existing keys to be used within the experiment
void collect_existing_entries(ExpEnv* _env, vector<string>& existing_keys ) {
  Options options;
  options.create_if_missing = false;
  options.use_direct_reads = false;
  options.num_levels = _env->rocksDB_max_levels;
  options.IncreaseParallelism(6);
  rocksdb::BlockBasedTableOptions table_options;
  options.num_levels = _env->rocksDB_max_levels;
  
  
  //file_size is defaulting to : std::numeric_limits<uint64_t>::max()
  FluidLSMTree* tree = new FluidLSMTree(_env->T, _env->K, _env->Z, _env->file_size, options);
  options.listeners.emplace_back(tree);


  DB* db = nullptr;
  Status s = DB::OpenForReadOnly(options, _env->path, &db);

  if (!s.ok()) {
    std::cerr << "Problem opening DB. Closing." << std::endl;
    delete db;
    exit(0);
  }

  tree->buildStructure(db);

  int num_existing_keys_to_get = _env->num_queries * (_env->nonzero_to_zero_ratio / (_env->nonzero_to_zero_ratio + 1));

  if (num_existing_keys_to_get == 0) {
    return;
  }

  struct timeval t1, t2;
  gettimeofday(&t1, NULL);

  string key_prefix = "";
  string val_prefix = "";
  //if (_env->target_level_for_non_zero_result_point_lookups != -1) {
  key_prefix = _env->key_prefix_for_entries_to_target_in_queries;
  val_prefix = to_string(_env->target_level_for_non_zero_result_point_lookups) + "-";
  //}
  //else {
  //  val_prefix = to_string(tree->largestOccupiedLevel()) + "-";
  //}


  // verify the values are still there
  existing_keys.reserve(num_existing_keys_to_get);
  std::string existing_key;
  std::string existing_val;
  auto iter = db->NewIterator(ReadOptions());
  while (existing_keys.size() < num_existing_keys_to_get) {
    string key = DataGenerator::generate_key(key_prefix);
    iter->Seek(key);
    if (iter->Valid()) {
      existing_key = iter->key().ToString();
      existing_val = iter->value().ToString();
      if (existing_val.compare(0, val_prefix.size(), val_prefix) == 0) {
        existing_keys.push_back(existing_key);
      }
    }
    //assert(value == std::string(500, 'a' + (i % 26)));
  }
  //double query_time = difftime(end_q, start_q);
  delete iter;
  gettimeofday(&t2, NULL);
  double experiment_time = (t2.tv_sec - t1.tv_sec) * 1000.0 + (t2.tv_usec - t1.tv_usec)/1000.0;
  experiment_time /= 1000.0;  // get it to be in seconds

  if (_env->debugging) {
    std::cerr << "collected " << num_existing_keys_to_get << " existing keys in " << experiment_time << "seconds" << endl;
    std::cerr << endl;
  }

  db->Close();
  delete db;
}
*/

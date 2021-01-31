/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */

#ifndef EMU_ENVIRONMENT_H_
#define EMU_ENVIRONMENT_H_


#include <iostream>
#include <fstream>
#include <chrono>
#include <ctime> 
#include <vector>

using namespace std;


class EmuEnv
{
private:
  EmuEnv(); 
  static EmuEnv *instance;

public:
  static EmuEnv* getInstance();

  double delete_persistence_latency;
  double* level_delete_persistence_latency;
  int* RR_level_last_file_selected;  // !YBS-sep06-XX!
  static void AddNewLevel(int _level_count, EmuEnv* _env); // !YBS-sep06-XX!
  static void ReSetLevelDeletePersistenceLatency(int _level_count, EmuEnv* _env); // reset dpl-per-level when there in a new level added
  static double GetLevelDeletePersistenceLatency(int _level, EmuEnv* _env);

  std::chrono::time_point<std::chrono::system_clock> oldest_delete_file_timestamp;
  int flag;
  std::vector<uint64_t> vec;
  static void DumpDeleteFileTimestamp(std::chrono::time_point<std::chrono::system_clock> delete_file_timestamp, uint64_t delete_file_id, EmuEnv* _env);
  static std::chrono::time_point<std::chrono::system_clock> GetDumpedDeleteFileTimestamp();
  static void PopulatingVector(uint64_t _file_id);
  static int CheckingVector(uint64_t _file_id);
  static void PrintRRIndices(EmuEnv* _env); // !YBS-sep06-XX!

  // First-Entry Flags (FEFs) // !YBS-sep06-XX!
  bool version_set_FEF; // !YBS-sep06-XX!

  int destroy_database;
  int clear_system_cache; // !YBS-sep09-XX!
  int show_progress; // !YBS-sep17-XX!
  char current_op; // !YBS-sep18-XX!

// Options set through command line 
  double size_ratio; // T ; used to set op->max_bytes_for_level_multiplier
  int buffer_size_in_pages; // P
  int entries_per_page; // B
  int entry_size; // E
  size_t buffer_size; // M = P*B*E ; in Bytes
  int file_to_memtable_size_ratio; // f
  uint64_t file_size; // F
  int verbosity;

  // adding new parameters with Guanting
  uint16_t compaction_pri; // c // 1 for kMinOverlappingRatio, 2 for kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for kOldestSmallestSeqFirst, 5 for kEffacingCompaction
  double bits_per_key; // b


// Options hardcoded in code
  // Memory allocation options
    int max_write_buffer_number; // max number of memtables in memory
    uint16_t memtable_factory; // 1 for skiplist, 2 for vector, 3 for hash skiplist, 4 for hash linklist


  // Compaction options
    long target_file_size_base; // file size in level base (usually level 1)
    bool level_compaction_dynamic_level_bytes; // related to BG-merging -- ensures last level is T times larger than the previous level 
    uint16_t compaction_style; // 1 for kCompactionStyleLevel, 2 for kCompactionStyleUniversal, 3 for kCompactionStyleFIFO, 4 for kCompactionStyleNone 
    bool disable_auto_compactions; // for manual comapction (when compactions_style = 4, i.e., kCompactionStyleNone)
    uint16_t compaction_filter; // 0 for nullptr, 1 for invoking custom compaction filter, if any
    uint16_t compaction_filter_factory; // 0 for nullptr, 1 for invoking custom compaction filter factory, if any
    int access_hint_on_compaction_start; // 1: NONE, 2: NORMAL, 3: SEQUENTIAL, 4: WILLNEED  https://linux.die.net/man/2/fadvise
    int level0_file_num_compaction_trigger; // number of files that triggers compaction in level0
    int target_file_size_multiplier; // multiplier for file size if we want to have same number of files per level
    int max_background_jobs; // Maximum number of concurrent background jobs (compactions and flushes).
    int max_compaction_bytes; // target_file_size_base * 25, limit number of bytes in one compaction to be lower than this threshold. But it’s not guaranteed.
    long max_bytes_for_level_base; // capacity for level base(usually level 1)
    int merge_operator;   // TBC
    int soft_pending_compaction_bytes_limit;    // TBC
    int hard_pending_compaction_bytes_limit;    // TBC
    int periodic_compaction_seconds;
    bool use_direct_io_for_flush_and_compaction;
    int live_levels; //!YBS-sep07-XX!
    int num_levels; // Maximum number of levels that a tree may have [RDB_default: 7]

  // TableOptions
    bool no_block_cache; // TBC
    int block_cache; 
    double block_cache_high_priority_ratio; //!YBS-sep09-XX!
    bool cache_index_and_filter_blocks;
    bool cache_index_and_filter_blocks_with_high_priority;    // Deprecated by no_block_cache
    int read_amp_bytes_per_bit;   // temporarily 4; why 4 ?
    uint16_t data_block_index_type; // 1 for kDataBlockBinarySearch, 2 for kDataBlockBinaryAndHash
    uint16_t index_type; // 1 for kHashSearch, 2 for kTwoLevelIndexSearch, 3 for kBinarySearchWithFirstKey
    bool partition_filters;
    int metadata_block_size;   // currently deprecated by data_block_index_type; TBC
    bool pin_top_level_index_and_filter; // TBC
    uint16_t index_shortening; // // {1: kNoShortening, 2: kShortenSeparators, 3: kShortenSeparatorsAndSuccessor}
    int block_size_deviation; // TBC
    bool  enable_index_compression; // TBC

  //Compression options
    uint16_t compression; // 1 for kNoCompression, 2 for kSnappyCompression, 3 for kZlibCompression, 4 for kBZip2Compression, 5 for kLZ4Compression, 6 for kLZ4HCCompression, 7 for kXpressCompression, 8 for kZSTD, 9 for kZSTDNotFinalCompression, 10 for kDisableCompressionOption

  // ReadOptions
    bool verify_checksums; // TBC
    bool fill_cache; // data block/index block read for this iteration will not be cached
    int iter_start_seqnum; // TBC
    bool ignore_range_deletions; // TBC
    uint16_t read_tier; // 1 for kReadAllTier, 2 for kBlockCacheTier, 3 for kPersistedTier, 4 for kMemtableTier

  // WriteOptions
    bool low_pri; // every insert is less important than compaction
    bool sync; // make every write wait for sync with log (so we see real perf impact of insert)
    bool disableWAL;
    bool no_slowdown; // enabling this will make some insertions fail 
    bool ignore_missing_column_families;

  // Others
    int max_open_files;    // TBC; default: -1 keep opening, or 20
    int max_file_opening_threads;

  // Other CFOptions
    uint16_t comparator;
    int max_sequential_skip_in_iterations;
    int memtable_prefix_bloom_size_ratio;    // disabled
    int level0_stop_writes_trigger;
    bool paranoid_file_checks;
    bool optimize_filters_for_hits;
    bool inplace_update_support;
    int inplace_update_num_locks;
    bool report_bg_io_stats;
    int max_successive_merges;   // read-modified-write related
    

  // Other DBOptions
    bool create_if_missing;
    int delayed_write_rate;
    int bytes_per_sync;
    int stats_persist_period_sec;
    bool enable_thread_tracking;
    int stats_history_buffer_size;
    bool allow_concurrent_memtable_write;
    bool dump_malloc_stats;
    bool use_direct_reads;
    bool avoid_flush_during_shutdown;
    bool advise_random_on_open;
    uint64_t delete_obsolete_files_period_micros; // 6 hours
    bool allow_mmap_reads;
    bool allow_mmap_writes;

    // Flush Options
    bool wait;
    bool allow_write_stall;


// options with default values
    // Memory allocation
      // op->max_write_buffer_number_to_maintain = 0;    // immediately freed after flushed
      // op->db_write_buffer_size = 0;   // disabled
      // op->arena_block_size = 0;
      // op->memtable_huge_page_size = 0;
      // op.sample_for_compression = 0;    // disabled
      // op.bottommost_compression = kDisableCompressionOption;

    // Compaction
      // op->min_write_buffer_number_to_merge = 1;
      // op->compaction_readahead_size = 0;
      // op->max_bytes_for_level_multiplier_additional = std::vector<int>(op->num_levels, 1);
      // op->max_subcompactions = 1;   // no subcomapctions
      // op->avoid_flush_during_recovery = false;
      // op->atomic_flush = false;
      // op->new_table_reader_for_compaction_inputs = false;   // forced to true when using direct_IO_read
      // compaction_options_fifo;

    // TableOptions
      // table_options.flush_block_policy_factory = nullptr;
      // table_options.block_align = false;
      // table_options.block_cache_compressed = nullptr;
      // table_options.block_restart_interval = 16;
      // table_options.index_block_restart_interval = 1;
      // table_options.format_version = 2;
      // table_options.verify_compression = false;
      // table_options.data_block_hash_table_util_ratio = 0.75;
      // table_options.checksum = kCRC32c;
      // table_options.whole_key_filtering = true;

    // Compression options
      // // L0 - L6: noCompression
      // op->compression_per_level = {CompressionType::kNoCompression,
      //                                   CompressionType::kNoCompression,
      //                                   CompressionType::kNoCompression,
      //                                   CompressionType::kNoCompression,
      //                                   CompressionType::kNoCompression,
      //                                   CompressionType::kNoCompression,
      //                                   CompressionType::kNoCompression};

    // ReadOptions
      // r_op->readahead_size = 0;
      // r_op->tailing = false;    
      // r_op->total_order_seek = false;
      // r_op->max_skippable_internal_keys = 0;
      // r_op->prefix_same_as_start = false;
      // r_op->pin_data = false;
      // r_op->background_purge_on_iterator_cleanup = false;
      // r_op->table_filter;   // assign a callback function
      // r_op->snapshot = nullptr;
      // r_op->iterate_lower_bound = nullptr;
      // r_op->iterate_upper_bound = nullptr;

    // Others
      // op->use_direct_io_for_flush_and_compaction = true;
      // options.avoid_flush_during_shutdown = true;
      // op.statistics = rocksdb::CreateDBStatistics();
      // ReadOptions::ignore_range_deletions = false;

    // Log Options
      // op.max_total_wal_size = 0;
      // op.db_log_dir = "";
      // op.max_log_file_size = 0;
      // op.wal_bytes_per_sync = 0;
      // op.strict_bytes_per_sync = false;
      // op.manual_wal_flush = false;
      // op.WAL_ttl_seconds = 0;
      // op.WAL_size_limit_MB = 0;
      // op.keep_log_file_num = 1000;
      // op.log_file_time_to_roll = 0;
      // op.recycle_log_file_num = 0;
      // op.info_log_level = nullptr;

    // Other CFOptions
      // op.prefix_extractor = nullptr;
      // op.bloom_locality = 0;
      // op.memtable_whole_key_filtering = false;
      // op.snap_refresh_nanos = 100 * 1000 * 1000;  
      // op.memtable_insert_with_hint_prefix_extractor = nullptr;
      // op.force_consistency_checks = false;

    // Other DBOptions
      // op.stats_dump_period_sec = 600;   // 10min
      // op.persist_stats_to_disk = false;
      // op.enable_pipelined_write = false;
      // op.table_cache_numshardbits = 6;
      // op.fail_if_options_file_error = false;
      // op.writable_file_max_buffer_size = 1024 * 1024;
      // op.write_thread_slow_yield_usec = 100;
      // op.enable_write_thread_adaptive_yield = true;
      // op.unordered_write = false;
      // op.preserve_deletes = false;
      // op.paranoid_checks = true;
      // op.two_write_queues = false;
      // op.use_fsync = true;
      // op.random_access_max_buffer_size = 1024 * 1024;
      // op.skip_stats_update_on_db_open = false;
      // op.error_if_exists = false;
      // op.manifest_preallocation_size = 4 * 1024 * 1024;
      // op.max_manifest_file_size = 1024 * 1024 * 1024;
      // op.is_fd_close_on_exec = true;
      // op.use_adaptive_mutex = false;
      // op.create_missing_column_families = false;
      // op.allow_ingest_behind = false;
      // op.avoid_unnecessary_blocking_io = false;
      // op.allow_fallocate = true;
      // op.allow_2pc = false;
      // op.write_thread_max_yield_usec = 100;

    
  // Workload options -- not sure if necessary to have these here!
    long num_inserts;

// old options
  
  string path;
  bool debugging;
  int FPR_optimization_level;
  int derived_num_levels;
  long N;
  long derived_N; 
  int K;
  int Z;
  int max_levels;
  int target_level_for_non_zero_result_point_lookups;
  bool use_block_based_filter;
  string key_prefix_for_entries_to_target_in_queries;
  string experiment_name;
  string experiment_starting_time;
  

  bool measure_IOs;
  bool print_IOs_per_file;
  long total_IOs;
  bool clean_caches_for_experiments;
  long file_system_page_size;
  bool only_tune;
  int num_read_query_sessions;
};

#endif /*EMU_ENVIRONMENT_H_*/


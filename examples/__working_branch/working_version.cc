/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */

#include <cstdio>
#include <string>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdlib>
#include "rocksdb/db.h"
//#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/table.h"
// #include "../db/version_set.h"
// #include "../db/version_edit.h"
#include "../util/string_util.h"
#include "args.hxx"
#include "emu_environment.h"
#include "stats.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/perf_context.h" // !YBS-sep01-XX!
#include "rocksdb/iostats_context.h" // !YBS-sep01-XX!
#include "aux_time.h" // !YBS-sep09-XX!
#include "thread" // !YBS-sep09-XX!
#include "chrono" // !YBS-sep09-XX!
#include "../util/cast_util.h"
#include "db/db_impl/db_impl.h"

using namespace rocksdb;

std::string kDBPath = "./db_working_home";
struct operation_tracker{
  long _inserts_completed = 0;
  long _updates_completed = 0;
  long _point_deletes_completed = 0;
  long _range_deletes_completed = 0;
  long _point_queries_completed = 0;
  long _range_queries_completed = 0;
} op_track;

int parse_arguments2(int argc, char *argv[], EmuEnv* _env);
void printExperimentalSetup(EmuEnv* _env); // !YBS-sep07-XX!
void printWorkloadStatistics(operation_tracker op_track); // !YBS-sep07-XX!
int runWorkload(EmuEnv* _env);
inline void showProgress(const uint32_t &n, const uint32_t &count);


int main(int argc, char *argv[]) {
  // check emu_environment.h for the contents of EmuEnv and also the definitions of the singleton experimental environment 
  EmuEnv* _env = EmuEnv::getInstance();
  Stats* fade_stats = Stats::getInstance();
  //parse the command line arguments
  if (parse_arguments2(argc, argv, _env)){
    exit(1);
  }

  if (_env->verbosity >= 4) {
    std::cout << "printing del_per_lat" << std::endl;
    for (int i = 0; i < 2; ++i){
      std::cout << i << ": " << EmuEnv::GetLevelDeletePersistenceLatency(i, _env) << std::endl;
    }
  }

  int s = runWorkload(_env); 

  fade_stats->printStats();

  return 0;
}


void configOptions(EmuEnv* _env, Options *op, BlockBasedTableOptions *t_op, WriteOptions *w_op, ReadOptions *r_op, FlushOptions *f_op) {
    // *op = Options();
    op->statistics = CreateDBStatistics(); // !YBS-sep01-XX!
    op->write_buffer_size = _env->buffer_size; // !YBS-sep07-XX!
    op->max_write_buffer_number = _env->max_write_buffer_number;   // min 2 // !YBS-sep07-XX!
    
    switch (_env->memtable_factory) {
      case 1:
        op->memtable_factory = std::shared_ptr<SkipListFactory>(new SkipListFactory); break;
      case 2:
        op->memtable_factory = std::shared_ptr<VectorRepFactory>(new VectorRepFactory); break;
      case 3:
        op->memtable_factory.reset(NewHashSkipListRepFactory()); break;
      case 4:
        op->memtable_factory.reset(NewHashLinkListRepFactory()); break;
      default:
        std::cerr << "error: memtable_factory" << std::endl;
    }

    // Compaction
    switch (_env->compaction_pri) {
      case 1:
        op->compaction_pri = kMinOverlappingRatio; break;
      case 2:
        op->compaction_pri = kByCompensatedSize; break;
      case 3:
        op->compaction_pri = kOldestLargestSeqFirst; break;
      case 4:
        op->compaction_pri = kOldestSmallestSeqFirst; break;
      case 5: 
        op->compaction_pri = kFADE; break;
      case 6: // !YBS-sep06-XX!
        op->compaction_pri = kRoundRobin; break; // !YBS-sep06-XX!
      case 7: // !YBS-sep07-XX!
        op->compaction_pri = kMinOverlappingGrandparent; break; // !YBS-sep07-XX!
      case 8: // !YBS-sep08-XX!
        op->compaction_pri = kFullLevel; break; // !YBS-sep08-XX!
      default:
        std::cerr << "ERROR: INVALID Data movement policy!" << std::endl;
    }

    op->max_bytes_for_level_multiplier = _env->size_ratio;
    op->allow_concurrent_memtable_write = _env->allow_concurrent_memtable_write;
    op->create_if_missing = _env->create_if_missing;
    op->target_file_size_base = _env->target_file_size_base;
    op->level_compaction_dynamic_level_bytes = _env->level_compaction_dynamic_level_bytes;
    switch (_env->compaction_style) {
      case 1:
        op->compaction_style = kCompactionStyleLevel; break;
      case 2:
        op->compaction_style = kCompactionStyleUniversal; break;
      case 3:
        op->compaction_style = kCompactionStyleFIFO; break;
      case 4:
        op->compaction_style = kCompactionStyleNone; break;
      default:
        std::cerr << "ERROR: INVALID Compaction eagerness!" << std::endl;
    }
    
    op->disable_auto_compactions = _env->disable_auto_compactions;
    if (_env->compaction_filter == 0) {
      ;// do nothing
    } 
    else {
      ;// invoke manual compaction_filter
    }
    if (_env->compaction_filter_factory == 0) {
      ;// do nothing
    } 
    else {
      ;// invoke manual compaction_filter_factory
    }
    switch (_env->access_hint_on_compaction_start) {
      case 1:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::NONE; break;
      case 2:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::NORMAL; break;
      case 3:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::SEQUENTIAL; break;
      case 4:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::WILLNEED; break;
      default:
        std::cerr << "error: access_hint_on_compaction_start" << std::endl;
    }
    
    if (op->compaction_style != kCompactionStyleUniversal) // !YBS-sep07-XX!
      op->level0_file_num_compaction_trigger = _env->level0_file_num_compaction_trigger; // !YBS-sep07-XX!
    op->target_file_size_multiplier = _env->target_file_size_multiplier;
    op->max_background_jobs = _env->max_background_jobs;
    op->max_compaction_bytes = _env->max_compaction_bytes;
    op->max_bytes_for_level_base = _env->buffer_size * _env->size_ratio;
    std::cout << "printing: max_bytes_for_level_base = " << op->max_bytes_for_level_base << " buffer_size = " << _env->buffer_size << " size_ratio = " << _env->size_ratio << std::endl;
    if (_env->merge_operator == 0) {
      ;// do nothing
    } 
    else {
      ;// use custom merge operator
    }
    op->soft_pending_compaction_bytes_limit = _env->soft_pending_compaction_bytes_limit;    // No pending compaction anytime, try and see
    op->hard_pending_compaction_bytes_limit = _env->hard_pending_compaction_bytes_limit;    // No pending compaction anytime, try and see
    op->periodic_compaction_seconds = _env->periodic_compaction_seconds;
    op->use_direct_io_for_flush_and_compaction = _env->use_direct_io_for_flush_and_compaction;
    if (op->compaction_style != kCompactionStyleUniversal) // !YBS-sep07-XX!
      op->num_levels = _env->num_levels; // !YBS-sep07-XX!


    //Compression
    switch (_env->compression) {
      case 1:
        op->compression = kNoCompression; break;
      case 2:
        op->compression = kSnappyCompression; break;
      case 3:
        op->compression = kZlibCompression; break;
      case 4:
        op->compression = kBZip2Compression; break;
      case 5:
      op->compression = kLZ4Compression; break;
      case 6:
      op->compression = kLZ4HCCompression; break;
      case 7:
      op->compression = kXpressCompression; break;
      case 8:
      op->compression = kZSTD; break;
      case 9:
      op->compression = kZSTDNotFinalCompression; break;
      case 10:
      op->compression = kDisableCompressionOption; break;

      default:
        std::cerr << "error: compression" << std::endl;
    }

  // table_options.enable_index_compression = kNoCompression;

  // Other CFOptions
  switch (_env->comparator) {
      case 1:
        op->comparator = BytewiseComparator(); break;
      case 2:
        op->comparator = ReverseBytewiseComparator(); break;
      case 3:
        // use custom comparator
        break;
      default:
        std::cerr << "error: comparator" << std::endl;
    }

  op->max_sequential_skip_in_iterations = _env-> max_sequential_skip_in_iterations;
  op->memtable_prefix_bloom_size_ratio = _env-> memtable_prefix_bloom_size_ratio;    // disabled
  op->level0_stop_writes_trigger = _env->level0_stop_writes_trigger;
  op->paranoid_file_checks = _env->paranoid_file_checks;
  op->optimize_filters_for_hits = _env->optimize_filters_for_hits;
  op->inplace_update_support = _env->inplace_update_support;
  op->inplace_update_num_locks = _env->inplace_update_num_locks;
  op->report_bg_io_stats = _env->report_bg_io_stats;
  op->max_successive_merges = _env->max_successive_merges;   // read-modified-write related

  //Other DBOptions
  op->create_if_missing = _env->create_if_missing;
  op->delayed_write_rate = _env->delayed_write_rate;
  op->max_open_files = _env->max_open_files;
  op->max_file_opening_threads = _env->max_file_opening_threads;
  op->bytes_per_sync = _env->bytes_per_sync;
  op->stats_persist_period_sec = _env->stats_persist_period_sec;
  op->enable_thread_tracking = _env->enable_thread_tracking;
  op->stats_history_buffer_size = _env->stats_history_buffer_size;
  // op->allow_concurrent_memtable_write = _env->allow_concurrent_memtable_write;
  op->dump_malloc_stats = _env->dump_malloc_stats;
  op->use_direct_reads = _env->use_direct_reads;
  op->avoid_flush_during_shutdown = _env->avoid_flush_during_shutdown;
  op->advise_random_on_open = _env->advise_random_on_open;
  op->delete_obsolete_files_period_micros = _env->delete_obsolete_files_period_micros;   // 6 hours
  op->allow_mmap_reads = _env->allow_mmap_reads;
  op->allow_mmap_writes = _env->allow_mmap_writes;

  //TableOptions
  // !YBS-sep09-XX
  if (_env->block_cache == 0) {
    t_op->no_block_cache = true;
    t_op->cache_index_and_filter_blocks = false;
  } // TBC
  else {
    t_op->no_block_cache = false;
    std::shared_ptr<Cache> cache = NewLRUCache(_env->block_cache*1024*1024, -1, false, _env->block_cache_high_priority_ratio);
    t_op->block_cache = cache;
    t_op->cache_index_and_filter_blocks = _env->cache_index_and_filter_blocks;
  }
  _env->no_block_cache = t_op->no_block_cache;
  // !END

  if (_env->bits_per_key == 0) {
    ;// do nothing
  } 
  else {
    t_op->filter_policy.reset(NewBloomFilterPolicy(_env->bits_per_key, false));    // currently build full filter instead of block-based filter
  }

  
  t_op->cache_index_and_filter_blocks_with_high_priority = _env->cache_index_and_filter_blocks_with_high_priority;    // Deprecated by no_block_cache
  t_op->read_amp_bytes_per_bit = _env->read_amp_bytes_per_bit;
  
  switch (_env->data_block_index_type) {
      case 1:
        t_op->data_block_index_type = BlockBasedTableOptions::kDataBlockBinarySearch; break;
      case 2:
        t_op->data_block_index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash; break;
      default:
        std::cerr << "error: TableOptions::data_block_index_type" << std::endl;
  }
  switch (_env->index_type) {
      case 1:
        t_op->index_type = BlockBasedTableOptions::kBinarySearch; break;
      case 2:
        t_op->index_type = BlockBasedTableOptions::kHashSearch; break;
      case 3:
        t_op->index_type = BlockBasedTableOptions::kTwoLevelIndexSearch; break;
      case 4:
        t_op->index_type = BlockBasedTableOptions::kBinarySearchWithFirstKey; break;
      default:
        std::cerr << "error: TableOptions::index_type" << std::endl;
  }
  t_op->partition_filters = _env->partition_filters;
  t_op->block_size = _env->entries_per_page * _env->entry_size;
  t_op->metadata_block_size = _env->metadata_block_size;
  t_op->pin_top_level_index_and_filter = _env->pin_top_level_index_and_filter;
  
  switch (_env->index_shortening) {
      case 1:
        t_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::kNoShortening; break;
      case 2:
        t_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators; break;
      case 3:
        t_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::kShortenSeparatorsAndSuccessor; break;
      default:
        std::cerr << "error: TableOptions::index_shortening" << std::endl;
  }
  t_op->block_size_deviation = _env->block_size_deviation;
  t_op->enable_index_compression = _env->enable_index_compression;
  // Set all table options
  op->table_factory.reset(NewBlockBasedTableFactory(*t_op));

  //WriteOptions
  w_op->sync = _env->sync; // make every write wait for sync with log (so we see real perf impact of insert) -- DOES NOT CAUSE SLOWDOWN
  w_op->low_pri = _env->low_pri; // every insert is less important than compaction -- CAUSES SLOWDOWN
  w_op->disableWAL = _env->disableWAL; 
  w_op->no_slowdown = _env->no_slowdown; // enabling this will make some insertions fail -- DOES NOT CAUSE SLOWDOWN
  w_op->ignore_missing_column_families = _env->ignore_missing_column_families;
  
  //ReadOptions
  r_op->verify_checksums = _env->verify_checksums;
  r_op->fill_cache = _env->fill_cache;
  r_op->iter_start_seqnum = _env->iter_start_seqnum;
  r_op->ignore_range_deletions = _env->ignore_range_deletions;
  switch (_env->read_tier) {
    case 1:
      r_op->read_tier = kReadAllTier; break;
    case 2:
      r_op->read_tier = kBlockCacheTier; break;
    case 3:
      r_op->read_tier = kPersistedTier; break;
    case 4:
      r_op->read_tier = kMemtableTier; break;
    default:
      std::cerr << "error: ReadOptions::read_tier" << std::endl;
  }

  //FlushOptions
  f_op->wait = _env->wait;
  f_op->allow_write_stall = _env->allow_write_stall;


// op->max_write_buffer_number_to_maintain = 0;    // immediately freed after flushed
    // op->db_write_buffer_size = 0;   // disable
    // op->arena_block_size = 0;
    // op->memtable_huge_page_size = 0;
    
  // Compaction options
    // op->min_write_buffer_number_to_merge = 1;
    // op->compaction_readahead_size = 0;
    // op->max_bytes_for_level_multiplier_additional = std::vector<int>(op->num_levels, 1);
    // op->max_subcompactions = 1;   // no subcomapctions
    // op->avoid_flush_during_recovery = false;
    // op->atomic_flush = false;
    // op->new_table_reader_for_compaction_inputs = false;   // forced to true when using direct_IO_read
    // compaction_options_fifo;
  
  // Compression options
    // // L0 - L6: noCompression
    // op->compression_per_level = {CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression};

  // op->sample_for_compression = 0;    // disabled
  // op->bottommost_compression = kDisableCompressionOption;

// Log Options
  // op->max_total_wal_size = 0;
  // op->db_log_dir = "";
  // op->max_log_file_size = 0;
  // op->wal_bytes_per_sync = 0;
  // op->strict_bytes_per_sync = false;
  // op->manual_wal_flush = false;
  // op->WAL_ttl_seconds = 0;
  // op->WAL_size_limit_MB = 0;
  // op->keep_log_file_num = 1000;
  // op->log_file_time_to_roll = 0;
  // op->recycle_log_file_num = 0;
  // op->info_log_level = nullptr;
  
// Other CFOptions
  // op->prefix_extractor = nullptr;
  // op->bloom_locality = 0;
  // op->memtable_whole_key_filtering = false;
  // op->snap_refresh_nanos = 100 * 1000 * 1000;  
  // op->memtable_insert_with_hint_prefix_extractor = nullptr;
  // op->force_consistency_checks = false;

// Other DBOptions
  // op->stats_dump_period_sec = 600;   // 10min
  // op->persist_stats_to_disk = false;
  // op->enable_pipelined_write = false;
  // op->table_cache_numshardbits = 6;
  // op->fail_if_options_file_error = false;
  // op->writable_file_max_buffer_size = 1024 * 1024;
  // op->write_thread_slow_yield_usec = 100;
  // op->enable_write_thread_adaptive_yield = true;
  // op->unordered_write = false;
  // op->preserve_deletes = false;
  // op->paranoid_checks = true;
  // op->two_write_queues = false;
  // op->use_fsync = true;
  // op->random_access_max_buffer_size = 1024 * 1024;
  // op->skip_stats_update_on_db_open = false;
  // op->error_if_exists = false;
  // op->manifest_preallocation_size = 4 * 1024 * 1024;
  // op->max_manifest_file_size = 1024 * 1024 * 1024;
  // op->is_fd_close_on_exec = true;
  // op->use_adaptive_mutex = false;
  // op->create_missing_column_families = false;
  // op->allow_ingest_behind = false;
  // op->avoid_unnecessary_blocking_io = false;
  // op->allow_fallocate = true;
  // op->allow_2pc = false;
  // op->write_thread_max_yield_usec = 100;*/

}

// inline void sleep_for_ms(uint32_t ms) {
//   std::this_thread::sleep_for(std::chrono::milliseconds(ms));
// }

// // Need to select timeout carefully
// // Completion not guaranteed
// bool FlushMemTableMayAllComplete(DB *db) {
//     uint64_t pending_flush;
//     uint64_t running_flush;
//     bool success = db->GetIntProperty("rocksdb.mem-table-flush-pending", &pending_flush)
//                               && db->GetIntProperty("rocksdb.num-running-flushes", &running_flush);
//     while (pending_flush || running_flush || !success) {
//         sleep_for_ms(200);
//         success = db->GetIntProperty("rocksdb.mem-table-flush-pending", &pending_flush)
//                               && db->GetIntProperty("rocksdb.num-running-flushes", &running_flush);
//     }
//     return (static_cast_with_check<DBImpl, DB>(db->GetRootDB()))
//             ->WaitForFlushMemTable( static_cast_with_check<ColumnFamilyHandleImpl>(db->DefaultColumnFamily())->cfd()).ok();
// } 

// // Need to select timeout carefully
// // Completion not guaranteed
// bool CompactionMayAllComplete(DB *db) {
//     uint64_t pending_compact;
//     uint64_t pending_compact_bytes;
//     uint64_t running_compact;
//     bool success = db->GetIntProperty("rocksdb.compaction-pending", &pending_compact)
//                               && db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pending_compact_bytes)
//                               && db->GetIntProperty("rocksdb.num-running-compactions", &running_compact);
//     while ((pending_compact &&  pending_compact_bytes != 0) || running_compact || !success) {
//         sleep_for_ms(200);
//         success = db->GetIntProperty("rocksdb.compaction-pending", &pending_compact)
//                               && db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pending_compact_bytes)
//                               && db->GetIntProperty("rocksdb.num-running-compactions", &running_compact);
//     }
//     return true;
// }

// Status CloseDB(DB *&db, const FlushOptions &flush_op) {
//     Status return_status = Status::Incomplete();
//     Status s = db->Flush(flush_op);
//     assert(s.ok());
//     if (FlushMemTableMayAllComplete(db) && CompactionMayAllComplete(db)) {
//         return_status = Status::OK();
//     }
//     s = db->Close();
//     assert(s.ok());
//     delete db;
//     db = nullptr;
//     std::cout << " OK" << std::endl;
//     return return_status;
// }

int runWorkload(EmuEnv* _env) {
  DB* db;
  Options options;
  WriteOptions w_options;
  ReadOptions r_options;
  BlockBasedTableOptions table_options;
  FlushOptions f_options;

  configOptions(_env, &options, &table_options, &w_options, &r_options, &f_options);

  // options.allow_concurrent_memtable_write = _env->allow_concurrent_memtable_write;
  // options.create_if_missing = _env->create_if_missing;

  if (_env->destroy_database) {
    DestroyDB(kDBPath, options); 
    std::cout << "Destroying database ..." << std::endl;
  }

  printExperimentalSetup(_env); // !YBS-sep07-XX!
  std::cout << "Maximum #OpenFiles = " << options.max_open_files << std::endl; // !YBS-sep07-XX!
  std::cout << "Maximum #ThreadsUsedToOpenFiles = " << options.max_file_opening_threads << std::endl; // !YBS-sep07-XX!
  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  Stats* fade_stats = Stats::getInstance();
  fade_stats->db_open = true;

  int current_level = fade_stats->levels_in_tree;
  
  // opening workload file for the first time 
  ifstream workload_file;
  workload_file.open("workload.txt");
  assert(workload_file);
  // doing a first pass to get the workload size
  uint32_t workload_size = 0;
  std::string line;
  while (std::getline(workload_file, line))
    ++workload_size;
  workload_file.close();

  // !YBS-sep09-XX!
  // Clearing the system cache
  if (_env->clear_system_cache) { 
    std::cout << "Clearing system cache ..." << std::endl; 
    system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'"); 
  }
  // START stat collection
  // if (keep_wl_stats)
  {
    // begin perf/iostat code
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_perf_context()->ClearPerLevelPerfContext();
    rocksdb::get_perf_context()->EnablePerLevelPerfContext();
    rocksdb::get_iostats_context()->Reset();
    // end perf/iostat code
  }
  if (options.compaction_style == kCompactionStyleUniversal)
    std::cout << "Compaction Eagerness: Tiering (kCompactionStyleUniversal)" << std::endl;
  else
    std::cout << "Compaction Eagerness: Hybrid leveling (kCompactionStyleLeveling)" << std::endl;
  // !END

  // re-opening workload file to execute workload
  workload_file.open("workload.txt");
  assert(workload_file);

  Iterator *it = db-> NewIterator(r_options); // for range reads
  uint32_t counter = 0; // for progress bar

  for (int i = 0; i<20; ++i) {
    _env->level_delete_persistence_latency[i] = -1;
    _env->RR_level_last_file_selected[i] = -1; // !YBS-sep06-XX!
  }
  // !YBS-sep09-XX!
  my_clock start_time, end_time;
  if (my_clock_get_time(&start_time) == -1) 
    std::cerr << "Failed to get experiment start time" << std::endl;
  // !END

  while(!workload_file.eof()) {
    char instruction;
    long key, start_key, end_key;
    string value;
    workload_file >> instruction;
    _env->current_op = instruction; // !YBS-sep18-XX!
    switch (instruction)
    {
    case 'I': // insert
      workload_file >> key >> value;
      // if (_env->verbosity == 2) std::cout << instruction << " " << key << " " << value << "" << std::endl;
      // Put key-value
      s = db->Put(w_options, ToString(key), value);
      if (!s.ok()) std::cerr << s.ToString() << std::endl;
      assert(s.ok());
      op_track._inserts_completed++;
      counter++;
      fade_stats->inserts_completed++;
      break;

    case 'U': // update
      workload_file >> key >> value;
      // if (_env->verbosity == 2) std::cout << instruction << " " << key << " " << value << "" << std::endl;
      // Put key-value
      s = db->Put(w_options, ToString(key), value);
      if (!s.ok()) std::cerr << s.ToString() << std::endl;
      assert(s.ok());
      op_track._updates_completed++;
      counter++;
      fade_stats->updates_completed++;
      break;

    case 'D': // point delete 
      workload_file >> key;
      // if (_env->verbosity == 2) std::cout << instruction << " " << key << "" << std::endl;
      s = db->Delete(w_options, ToString(key));
      assert(s.ok());
      op_track._point_deletes_completed++;
      counter++;
      fade_stats->point_deletes_completed++;
      break;

    case 'R': // range delete 
      workload_file >> start_key >> end_key;
      // if (_env->verbosity == 2) std::cout << instruction << " " << start_key << " " << end_key << "" << std::endl;
      s = db->DeleteRange(w_options, ToString(start_key), ToString(end_key));
      assert(s.ok());
      op_track._range_deletes_completed++;
      counter++;
      fade_stats->range_deletes_completed++;
      break;

    case 'Q': // probe: point query
      workload_file >> key;
      // if (_env->verbosity == 2) std::cout << instruction << " " << key << "" << std::endl;
      s = db->Get(r_options, ToString(key), &value);
      //if (!s.ok()) std::cerr << s.ToString() << "key = " << key << std::endl;
      // assert(s.ok());
      op_track._point_queries_completed++;
      counter++;
      fade_stats->point_queries_completed++;
      break;

    case 'S': // scan: range query
      workload_file >> start_key >> end_key;
      //std::cout << instruction << " " << start_key << " " << end_key << "" << std::endl;
      it->Refresh();
      assert(it->status().ok());
      for (it->Seek(ToString(start_key)); it->Valid(); it->Next()) {
        //std::cout << "found key = " << it->key().ToString() << std::endl;
        if(it->key().ToString() == ToString(end_key)) {
          break;
        }
      }
      if (!it->status().ok()) {
        std::cerr << it->status().ToString() << std::endl;
      }

      op_track._range_queries_completed++;
      counter++;
      fade_stats->range_queries_completed++;
      break;
    
    default:
      std::cerr << "ERROR: Case match NOT found !!" << std::endl;
      break;
    }

    if (workload_size < 100) workload_size = 100;
    if(counter % (workload_size/100) == 0 && _env->show_progress){
        showProgress(workload_size, counter);
    } 
  }

  fade_stats->completion_status = true;

  // // Close DB in a way of detecting errors
  // // followed by deleting the database object when examined to determine if there were any errors.
  // // Regardless of errors, it will release all resources and is irreversible.
  // // Flush the memtable before close
  // Status CloseDB(DB *&db, const FlushOptions &flush_op);
  
  workload_file.close();
  s = db->Close();
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  delete db;
  fade_stats->db_open = false;
  std::cout << "\n----------------------Closing DB-----------------------" 
          << " completion_status = " << fade_stats->completion_status << std::endl;

  // reopening (and closing the DB to flush LOG to .sst file)
  s = DB::Open(options, kDBPath, &db);
  fade_stats->db_open = true;
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  s = db->Close();
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  std::cout << "End of experiment - TEST !!\n";

  // !YBS-sep09-XX!
  if (my_clock_get_time(&end_time) == -1) 
    std::cerr << "Failed to get experiment end time" << std::endl;

  fade_stats->exp_runtime = getclock_diff_ns(start_time, end_time);
  // !END

  // !YBS-sep01-XX
  // STOP stat collection & PRINT stat
  // if (keep_wl_stats)
  {
    // sleep(5);
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    std::cout << "RocksDB Perf Context : " << std::endl;
    std::cout << rocksdb::get_perf_context()->ToString() << std::endl;
    std::cout << "RocksDB Iostats Context : " << std::endl;
    std::cout << rocksdb::get_iostats_context()->ToString() << std::endl;
    // END ROCKS PROFILE
    // Print Full RocksDB stats
    std::cout << "RocksDB Statistics : " << std::endl;
    std::cout << options.statistics->ToString() << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    // std::string tr_mem;
    // db->GetProperty("rocksdb.estimate-table-readers-mem", &tr_mem);
    // // Print Full RocksDB stats
    // std::cout << "RocksDB Estimated Table Readers Memory (index, filters) : " << tr_mem << std::endl;
  }
  // !END

  printWorkloadStatistics(op_track);

  return 1;
}



int parse_arguments2(int argc, char *argv[], EmuEnv* _env) {
  args::ArgumentParser parser("RocksDB_parser.", "");

  args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);
/*
  args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::AtMostOne);
  args::Group group2(parser, "Path is needed:", args::Group::Validators::All);
  args::Group group3(parser, "This group is all exclusive (either N or L):", args::Group::Validators::Xor);
  args::Group group4(parser, "Optional switches and parameters:", args::Group::Validators::DontCare);
  args::Group group5(parser, "Optional less frequent switches and parameters:", args::Group::Validators::DontCare);
*/

  args::ValueFlag<int> destroy_database_cmd(group1, "d", "Destroy and recreate the database [def: 1]", {'d', "destroy"});
  args::ValueFlag<int> clear_system_cache_cmd(group1, "cc", "Clear system cache [def: 1]", {"cc"}); // !YBS-sep09-XX!

  args::ValueFlag<int> size_ratio_cmd(group1, "T", "The number of unique inserts to issue in the experiment [def: 10]", {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(group1, "P", "The number of unique inserts to issue in the experiment [def: 4096]", {'P', "buffer_size_in_pages"});
  args::ValueFlag<int> entries_per_page_cmd(group1, "B", "The number of unique inserts to issue in the experiment [def: 4]", {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(group1, "E", "The number of unique inserts to issue in the experiment [def: 1024 B]", {'E', "entry_size"});
  args::ValueFlag<long> buffer_size_cmd(group1, "M", "The number of unique inserts to issue in the experiment [def: 16 MB]", {'M', "memory_size"});
  args::ValueFlag<int> file_to_memtable_size_ratio_cmd(group1, "file_to_memtable_size_ratio", "The number of unique inserts to issue in the experiment [def: 1]", {'f', "file_to_memtable_size_ratio"});
  args::ValueFlag<long> file_size_cmd(group1, "file_size", "The number of unique inserts to issue in the experiment [def: 256 KB]", {'F', "file_size"});
  args::ValueFlag<int> verbosity_cmd(group1, "verbosity", "The verbosity level of execution [0,1,2; def: 0]", {'V', "verbosity"});
  args::ValueFlag<int> compaction_pri_cmd(group1, "compaction_pri", "[Compaction priority: 1 for kMinOverlappingRatio, 2 for kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for kOldestSmallestSeqFirst; def: 1]", {'c', "compaction_pri"});
  args::ValueFlag<int> compaction_style_cmd(group1, "compaction_style", "[Compaction priority: 1 for kCompactionStyleLevel, 2 for kCompactionStyleUniversal, 3 for kCompactionStyleFIFO, 4 for kCompactionStyleNone; def: 1]", {'C', "compaction_style"}); // !YBS-sep07-XX!
  args::ValueFlag<int> bits_per_key_cmd(group1, "bits_per_key", "The number of bits per key assigned to Bloom filter [def: 10]", {'b', "bits_per_key"});
  args::ValueFlag<int> block_cache_cmd(group1, "bb", "Block cache size in MB [def: 8 MB]", {"bb"}); // !YBS-sep09-XX!
  args::ValueFlag<int> show_progress_cmd(group1, "show_progress", "Show progress [def: 0]", {'s', "sp"}); // !YBS-sep17-XX!

  args::ValueFlag<long> num_inserts_cmd(group1, "inserts", "The number of unique inserts to issue in the experiment [def: 0]", {'i', "inserts"});


  try {
      parser.ParseCLI(argc, argv);
  }
  catch (args::Help&) {
      std::cout << parser;
      exit(0);
      // return 0;
  }
  catch (args::ParseError& e) {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }
  catch (args::ValidationError& e) {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }

  _env->destroy_database = destroy_database_cmd ? args::get(destroy_database_cmd) : 1;
  _env->clear_system_cache = clear_system_cache_cmd ? args::get(clear_system_cache_cmd) : 1; // !YBS-sep09-XX!

  _env->size_ratio = size_ratio_cmd ? args::get(size_ratio_cmd) : 10;
  _env->buffer_size_in_pages = buffer_size_in_pages_cmd ? args::get(buffer_size_in_pages_cmd) : 4096;
  _env->entries_per_page = entries_per_page_cmd ? args::get(entries_per_page_cmd) : 4;
  _env->entry_size = entry_size_cmd ? args::get(entry_size_cmd) : 1024;
  _env->buffer_size = buffer_size_cmd ? args::get(buffer_size_cmd) : _env->buffer_size_in_pages * _env->entries_per_page * _env->entry_size;
  _env->file_to_memtable_size_ratio = file_to_memtable_size_ratio_cmd ? args::get(file_to_memtable_size_ratio_cmd) : 1;
  _env->file_size = file_size_cmd ? args::get(file_size_cmd) : _env->buffer_size;
  _env->verbosity = verbosity_cmd ? args::get(verbosity_cmd) : 0;
  _env->compaction_pri = compaction_pri_cmd ? args::get(compaction_pri_cmd) : 1;
  _env->compaction_style = compaction_style_cmd ? args::get(compaction_style_cmd) : 1; // !YBS-sep07-XX!
  _env->bits_per_key = bits_per_key_cmd ? args::get(bits_per_key_cmd) : 10;
  _env->block_cache = block_cache_cmd ? args::get(block_cache_cmd) : 8; // !YBS-sep09-XX!
  _env->show_progress = show_progress_cmd ? args::get(show_progress_cmd) : 0; // !YBS-sep17-XX!

  _env->num_inserts = num_inserts_cmd ? args::get(num_inserts_cmd) : 0;

  _env->target_file_size_base = _env->buffer_size; // !YBS-sep07-XX!
  _env->max_bytes_for_level_base = _env->buffer_size * _env->size_ratio; // !YBS-sep07-XX!


  return 0;
}


// !YBS-sep07-XX
void printExperimentalSetup(EmuEnv* _env) {
  int l = 10;
  
  std::cout 
  << std::setfill(' ') << std::setw(l) << "cmpt_sty" // !YBS-sep07-XX!
  << std::setfill(' ') << std::setw(l) << "cmpt_pri" 
  << std::setfill(' ') << std::setw(4) << "T" 
  << std::setfill(' ') << std::setw(l) << "P" 
  << std::setfill(' ') << std::setw(l) << "B" 
  << std::setfill(' ') << std::setw(l) << "E" 
  << std::setfill(' ') << std::setw(l) << "M" 
  // << std::setfill(' ') << std::setw(l)  << "f" 
  << std::setfill(' ') << std::setw(l) << "file_size" 
  << std::setfill(' ') << std::setw(l) << "L1_size" 
  << std::setfill(' ') << std::setw(l) << "blk_cch" // !YBS-sep09-XX!
  << std::setfill(' ') << std::setw(l) << "BPK" << "\n";
  std::cout << std::setfill(' ') << std::setw(l) << _env->compaction_style; // !YBS-sep07-XX!
  std::cout << std::setfill(' ') << std::setw(l) << _env->compaction_pri;
  std::cout << std::setfill(' ') << std::setw(4) << _env->size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size_in_pages;  
  std::cout << std::setfill(' ') << std::setw(l) << _env->entries_per_page;
  std::cout << std::setfill(' ') << std::setw(l) << _env->entry_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size;
  // std::cout << std::setfill(' ') << std::setw(l) << _env->file_to_memtable_size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->file_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->max_bytes_for_level_base;
  std::cout << std::setfill(' ') << std::setw(l) << _env->block_cache; // !YBS-sep09-XX!
  std::cout << std::setfill(' ') << std::setw(l) << _env->bits_per_key;

  std::cout << std::endl;
}

void printWorkloadStatistics(operation_tracker op_track) {
  int l = 10;
  
  std::cout << std::setfill(' ') 
  << std::setfill(' ') << std::setw(l) << "#I" 
  << std::setfill(' ') << std::setw(l) << "#U" 
  << std::setfill(' ') << std::setw(l) << "#PD" 
  << std::setfill(' ') << std::setw(l) << "#RD" 
  << std::setfill(' ') << std::setw(l) << "#PQ" 
  << std::setfill(' ') << std::setw(l) << "#RQ" << "\n";

  std::cout << std::setfill(' ') << std::setw(l) << op_track._inserts_completed;
  std::cout << std::setfill(' ') << std::setw(l) << op_track._updates_completed;
  std::cout << std::setfill(' ') << std::setw(l) << op_track._point_deletes_completed;
  std::cout << std::setfill(' ') << std::setw(l) << op_track._range_deletes_completed;
  std::cout << std::setfill(' ') << std::setw(l) << op_track._point_queries_completed;
  std::cout << std::setfill(' ') << std::setw(l) << op_track._range_queries_completed;

  std::cout << std::endl;
}
// !END

inline void showProgress(const uint32_t &workload_size, const uint32_t &counter) {
  
    // std::cout << "flag = " << flag << std::endl;
    // std::cout<<"2----";

    if (counter / (workload_size/100) >= 1) {
      for (int i = 0; i<104; i++){
        std::cout << "\b";
        fflush(stdout);
      }
    }
    for (int i = 0; i<counter / (workload_size/100); i++){
      std::cout << "=" ;
      fflush(stdout);
    }
    std::cout << std::setfill(' ') << std::setw(101 - counter / (workload_size/100));
    std::cout << counter*100/workload_size << "%";
      fflush(stdout);

  if (counter == workload_size) {
    std::cout << "\n";
    return;
  }
}


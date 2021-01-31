/*
 *  Created on: Sep 21, 2019
 *  Author: Subhadeep
 */


#ifndef STATS_H_
#define STATS_H_


#include <iostream>
#include <vector> 

using namespace std;

struct FadeFileInfo{
  std::string file_id;
  int file_level;
  long file_entry_count;
  long file_tombstone_count;
  long file_size;
  double file_age;
  bool file_ttl_expired;
};

class Stats
{
private:
  Stats(); 
  static Stats *instance;

public:
  static Stats* getInstance();
  // operation stats
  uint64_t key_size;
  uint64_t value_size;
  long inserts_completed;
  long updates_completed;
  float update_proportion;
  long point_deletes_completed;
  long range_deletes_completed;
  long point_queries_completed;
  long range_queries_completed;

  // current tree stats
  int levels_in_tree;
  long files_in_tree;
  long key_values_in_tree;
  long point_tombstones_in_tree;
  long range_tombstones_in_tree;

  // current file stats
  std::vector<int> files_in_level;
  std::vector<FadeFileInfo> fade_file_info;

  // compaction stats
  long compaction_count;
  long compactions_by_pointer_manipulation;
  long files_read_for_compaction;
  long bytes_read_for_compaction;
  long files_written_after_compaction;
  long bytes_written_after_compaction;

  // consolidated stats
  float space_amp;
  float write_amp;

  // latency stats
  long exp_runtime; // !YBS-sep09-XX!

  // misc stats
  bool completion_status;
  bool db_open;

  void printStats();


};

#endif /*STATS_H_*/



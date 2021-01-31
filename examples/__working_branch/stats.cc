/*
 *  Created on: Sep 21, 2019
 *  Author: Subhadeep
 */

#include <iostream>
#include <cmath>
#include <sys/time.h>
#include <iomanip>
#include <cstdlib>
#include "stats.h"


Stats* Stats::instance = 0;

Stats::Stats() 
{
  key_size = -1;
  value_size = -1;
  inserts_completed = -1;
  updates_completed = -1;
  update_proportion = -1;
  point_deletes_completed = -1;
  range_deletes_completed = -1;
  point_queries_completed = -1;
  range_queries_completed = -1;
  
  // current tree stats
  levels_in_tree = -1;
  files_in_tree = -1;
  key_values_in_tree = -1;
  point_tombstones_in_tree = -1;
  range_tombstones_in_tree = -1;

  // current file stats
  files_in_level.resize(0);
  fade_file_info.resize(0);

  // compaction stats
  compaction_count = -1;
  compactions_by_pointer_manipulation = -1;
  files_read_for_compaction = -1;
  bytes_read_for_compaction = -1;
  files_written_after_compaction = -1;
  bytes_written_after_compaction = -1;

  // consolidated stats
  space_amp = -1;
  write_amp = -1;

  // latency stats
  long exp_runtime = -1; // !YBS-sep09-XX!

  completion_status = false;
  db_open = false;
}

void Stats::printStats() {
  int l = 12;

  std::cout << std::setfill(' ') << std::setw(l-3) << "#p_ts_in_tree" 
  << std::setfill(' ') << std::setw(l) << "#kv_in_tree" 
  << std::setfill(' ') << std::setw(l) << "#I_done" 
  << std::setfill(' ') << std::setw(l) << "L_in_tree" 
  << std::setfill(' ') << std::setw(l) << "#U_done" 
  << std::setfill(' ') << std::setw(l) << "#PD_done" 
  << std::setfill(' ') << std::setw(l) << "#cmpt" 
  << std::setfill(' ') << std::setw(l) << "#cmpt_easy" 
  << std::setfill(' ') << std::setw(l) << "fls_rd_cmpt" 
  << std::setfill(' ') << std::setw(l) << "fls_wr_cmpt" 
  << std::setfill(' ') << std::setw(l) << "bts_rd_cmpt" 
  << std::setfill(' ') << std::setw(l) << "bts_wr_cmpt" 
  << "\n";
  std::cout << std::setfill(' ') << std::setw(l) << point_tombstones_in_tree;
  std::cout << std::setfill(' ') << std::setw(l) << key_values_in_tree;
  std::cout << std::setfill(' ') << std::setw(l) << inserts_completed;
  std::cout << std::setfill(' ') << std::setw(l) << levels_in_tree; 
  std::cout << std::setfill(' ') << std::setw(l) << updates_completed;
  std::cout << std::setfill(' ') << std::setw(l) << point_deletes_completed;
  std::cout << std::setfill(' ') << std::setw(l) << compaction_count;
  std::cout << std::setfill(' ') << std::setw(l) << compactions_by_pointer_manipulation;
  std::cout << std::setfill(' ') << std::setw(l) << files_read_for_compaction;
  std::cout << std::setfill(' ') << std::setw(l) << files_written_after_compaction;
  std::cout << std::setfill(' ') << std::setw(l) << bytes_read_for_compaction;
  std::cout << std::setfill(' ') << std::setw(l) << bytes_written_after_compaction;

  std::cout << std::endl;

  std::cout << "files in tree = " << files_in_tree << std::endl;
  for (int i=0; i<files_in_tree; ++i)
    // if (fade_file_info[i].file_ttl_expired)
    // std::cout << "file_id = " << fade_file_info[i].file_id << " level = " << fade_file_info[i].file_level 
    //         << " entry_count = " << fade_file_info[i].file_entry_count << " tombstone_count = " << fade_file_info[i].file_tombstone_count 
    //         << " file_size = " << fade_file_info[i].file_size << " file_age = " << fade_file_info[i].file_age 
    //         << " file_ttl_exp = " << fade_file_info[i].file_ttl_expired << "\n";

  // std::cout << "point_tombstones_in_tree = " << point_tombstones_in_tree << std::endl;
  // std::cout << "updates_completed = " << updates_completed << std::endl;
  // std::cout << "inserts_completed = " << inserts_completed << std::endl;
  update_proportion = (float) updates_completed / (float) inserts_completed;
  // std::cout << "update_proportion = " << update_proportion << std::endl;
  float superfluous_bytes = (float) point_tombstones_in_tree * ( (key_size + 1));// + (key_size + value_size)*(1 + update_proportion) );
  float unique_bytes = (inserts_completed - point_deletes_completed) * (key_size + value_size);
  space_amp = 100 * superfluous_bytes / unique_bytes; 

  // std::cout << "superfluous_bytes = " << superfluous_bytes << std::endl;
  // std::cout << "unique_bytes = " << unique_bytes << std::endl;

  std::cout << std::setfill(' ') << std::setw(l-3) 
    << "\%space_amp" << std::setfill(' ') << std::setw(l) 
    << "\%write_amp" << std::setfill(' ') << std::setw(l+7) 
    << "exp_runtime (ms)" << "\n"; // !YBS-sep09-XX!
  std::cout << std::setfill(' ') << std::setw(l-3) << space_amp;
  std::cout << std::setfill(' ') << std::setw(l) << write_amp;
  std::cout << std::setfill(' ') << std::setw(l+7) << std::fixed << std::setprecision(2) // !YBS-sep09-XX!
    << static_cast<double>(exp_runtime)/1000000; // !YBS-sep09-XX!
  std::cout << std::endl;

}

Stats* Stats::getInstance()
{
  if (instance == 0)
    instance = new Stats();

  return instance;
}


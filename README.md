<H1> Experimental codebase: "Constructing and Analyzing the LSM Compaction Design Space" </H1>

This repository is the modified version of RocksDB which contains additional compaction strategies and was used to run the experiments for our latest work: "Constructing and Analyzing the LSM Compaction Design Space". 

For our analysis and experimentation, we select nine representative compaction strategies that are prevalent in production and academic LSM-based systems. These strategies capture the wide variety of the possible compaction designs. We codify and present these candidate compaction strategies in Table 2. Full represents the classical compaction strategy for leveled LSM- trees that compacts two consecutive levels upon invocation. LO+1 and LO+2 denote two partial compaction routines that choose a file for compaction with the smallest overlap with files in the parent (i + 1) and grandparent (i + 2) level, respectively. RR chooses files for compaction in a round-robin fashion from each level. Cold and Old are read-friendly compaction strategies that mark the coldest and oldest file(s) in a level for compaction, respectively. TSD and TSA are delete-driven compaction strategies with triggers and data movement policies that are determined by the density of tombstones and the age of the oldest tombstone contained in a file, respectively. Tier represents the variant of tiering compaction with a trigger of space amplification. Finally, 1-Lvl represents the hybrid data layout, where the first level is implemented as tiering and the remaining ones as leveling. 

This work is accepted for publication in PVLDB 2021. You can access a [technical report here] (https://disc-projects.bu.edu/documents/DiSC-TR-LSM-Compaction-Analysis.pdf). 

<H1> Workload Generation </H1>
To run our experiments we also implemented our own workload generator: https://github.com/BU-DiSC/K-V-Workload-Generator
To generate a workload, simply give 

```
make
./load_gen
```

with the desired parameters. These include: Number of inserts, updates, deletes, point & range lookups, distribution styles, etc. 

<H1> Quick How-To </H1>
To run a simple experiment you can follow this guide:

First, clone the workload generator reposirtory and generate a workload. A simple command would be: 

```
./load_gen -I10000 -Q3000
```

More options are available. Type:

```
./load_gen --help 
```

for more details.

This will generate a workload with 10000 inserts and 3000 point-queries on existing keys. 

Then, move the newly generated workload.txt file in LSM-Compaction-Analysis/examples/\_\_working_branch/ where our API resides. Make sure that the LSM-Compaction-Analysis and the K-V-Workload-Generator repositories are located in the same path.

```
cp workload.txt ../LSM-Compaction-Analysis/examples/__working_branch/
```

After that, make sure that you have also compiled RocksDB. To do that navigate to LSM-Compaction-Analysis/ and run:
```
make static_lib
```

To run the workload, just give:

```
make
./working_version
```

More options are available. Type:

```
./working_version --help 
```

for more details.

At the end of the experiment you should be able to see a newly created log_home folder that contains all the results. More specifically, the output.csv file should contain all the results. To interpret this file, you also need to have a look at the tuple_map.txt file here https://github.com/BU-DiSC/LSM-Compaction-Analysis/blob/master/examples/__working_branch/tuple_map.txt. 


<H1> RocksDB Modifications </H1>

The modified file along with their location are listed below:

```
> external_sst_file_ingestion_job.cc db/ 
> flush_job.cc db/ 
> import_column_family_job.cc db/ 
> repair.cc db/ 
> version_edit.h db/ 
> version_set.cc db/ 
> version_set.h db/ 
> compaction_job.cc db/compaction/ 
> compaction.cc db/compaction/ 
> db_impl_compaction_flush.cc db/db_impl/ 
> db_impl_experimental.cc db/db_impl/ 
> db_impl_open.cc db/db_impl/ 
> emu_environment.cc examples/__working_branch/ 
> emu_environment.h examples/__working_branch/ 
> stats.cc examples/ working_branch/ 
> stats.h examples/ working_branch/
> working_version.cc examples/__working_branch/ 
> advanced_options.h include/rocksdb/
> db.h include/rocksdb/
> listener.h include/rocksdb/
> metadata.h include/rocksdb/
> portal.h java/rocksjni/
> options_helper.cc options/
```


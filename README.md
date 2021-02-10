<H1> Experimental codebase for "Constructing and Analyzing the LSM Compaction Design Space" </H1>

This repository contains the modified version of RocksDB which contains additional compaction strategies and was used to run the experiments for our latest work: "Constructing and Analyzing the LSM Compaction Design Space". You can access a [technical report here](https://disc-projects.bu.edu/documents/DiSC-TR-LSM-Compaction-Analysis.pdf).

For our analysis and experimentation, we select nine representative compaction strategies that are prevalent in production and academic LSM-based systems. These strategies capture the wide variety of the possible compaction designs. We codify and present these candidate compaction strategies in Table 2. Full represents the classical compaction strategy for leveled LSM- trees that compacts two consecutive levels upon invocation. LO+1 and LO+2 denote two partial compaction routines that choose a file for compaction with the smallest overlap with files in the parent (i + 1) and grandparent (i + 2) level, respectively. RR chooses files for compaction in a round-robin fashion from each level. Cold and Old are read-friendly compaction strategies that mark the coldest and oldest file(s) in a level for compaction, respectively. TSD and TSA are delete-driven compaction strategies with triggers and data movement policies that are determined by the density of tombstones and the age of the oldest tombstone contained in a file, respectively. Finally, Tier represents the variant of tiering compaction with a trigger of space amplification.

<H1> Workload generator </H1>
To run our experiments we also implemented our own workload generator: https://github.com/BU-DiSC/K-V-Workload-Generator
To generate a workload, simply give 

```
make
./load_gen
```

with the desired parameters. These include: Number of inserts, updates, deletes, updates, point & range lookups, distribution styles, etc. 

<H1> Quck How-To </H1>
To run a spimple experiment you can follow this simple guide:

First, clone the workload generator reposirtory and generate a workload. A simple command would be: 

```
$./load_gen -I10000 -Q3000
```

This will generate a workload with 10000 inserts and 3000 queries on existing keys. 
Then, move the newly generated workload.txt file in LSM-Compaction-Analysis/examples/__working_branch/ where our API resides. 
To run the workload, just give:

```
make
./working_version
```

Before that make sure that you have also compiled RocksDB. To do that navigate to LSM-Compaction-Analysis/ and run $make static_lib
At the end of the experiment you should be able to see a newly created log_home folder that contains all the results. More specifically, the output.csv file should contain all the results. To interpret this file, you also need to have a look at the tuple_map.txt file here https://github.com/BU-DiSC/LSM-Compaction-Analysis/blob/master/examples/__working_branch/tuple_map.txt. 

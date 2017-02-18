Edge Partitioning Algorithms for Large Graphs
=============================================

In this repo, we implement several edge partitioning algorithms and compute
their replication factors for comparison:

*   Random partitioning (random)
*   Degree-based hashing (DBH): a
    [paper](http://papers.nips.cc/paper/5396-distributed-power-law-graph-computing-theoretical-and-empirical-analysis.pdf)
    on NIPS'14
*   A method based on
    [Hilber space-filling curve](https://en.wikipedia.org/wiki/Hilbert_curve) (HSFC):
    this one is inspired by Frank McSherry's [post](https://github.com/frankmcsherry/blog/blob/master/posts/2015-01-15.md)
*   Our algorithms described in our paper **Graph Edge Partitioning via Neighborhood Heuristic** (submitted to KDD'17)
    -   Neighbor expansion (NE)
    -   Streaming neighbor expansion (SNE)

Compilation and Usage
---------------------

We tested our program on Ubuntu 14.04/16.04, and it requires the following
libraries: `glog`, `gflags`, `boost`.

Compile:
```
mkdir release && cd release
cmake ..
make -j8
```

Usage:
```
$ ./main --help
main: -filename <path to the input graph> [-filetype <edgelist|adjlist>] [-p <number of partitions>] [-memsize <memory budget in MB>]

  Flags from /home/qliu/workspace/edgepart/src/main.cpp:
    -filename (the file name of the input graph) type: string default: ""
    -filetype (the type of input file (supports 'edgelist' and 'adjlist'))
      type: string default: "edgelist"
    -inmem (in-memory mode) type: bool default: false
    -memsize (memory size in megabytes) type: uint64 default: 4096
    -method (partition method: ne, sne, random, and dbh) type: string
      default: "sne"
    -p (number of parititions) type: int32 default: 10
    -sample_ratio (the sample size divided by num_vertices) type: double
      default: 2
```

**Example.** Partition the Orkut graph into 30 parts using our NE algorithm:
```
$ ./main -p 30 -method ne -filename /path/to/com-orkut.ungraph.txt
```

**Example.** Partition the LiveJournal graph into 30 parts using our SNE
algorithm (`CacheSize = 2|V|`, see our paper for detailed description):
```
$ ./main -p 30 -method sne -filename /path/to/com-lj.ungraph.txt -sample_ratio 2
```

Evaluation
----------

The experiments are conducted on a PC with 16 GB RAM. MLE means "memory limit
exceeded".

Algorithms that are listed but not contained in this repo:

*   [METIS](http://glaros.dtc.umn.edu/gkhome/metis/metis/overview)
*   [Sheep](https://github.com/dmargo/sheep): published on VLDB'15
*   Algorithms that has been integrated in to
    [PowerGraph](https://github.com/jegonzal/PowerGraph)
    -   Oblivious
    -   High-Degree (are) Replicated First (HDRF): published on CIKM'15

Algorithm  |  wiki-Vote  |  email-Enron  |  web-Google  |  com-LiveJournal  |  com-Orkut  |  twitter-2010  |  com-Friendster  |  uk-union
---------  |  ---------  |  -----------  |  ----------  |  ---------------  |  ---------  |  ------------  |  --------------  |  --------
METIS      |  5.25       |  2.40         |  1.05        |  2.13             |  MLE        |  MLE           |  MLE             |  MLE
NE         |  2.35       |  1.34         |  1.12        |  1.55             |  2.48       |  1.88          |  1.98            |  1.04
Random     |  9.28       |  5.10         |  6.54        |  8.27             |  19.48      |  11.68         |  11.84           |  15.99
DBH        |  5.43       |  3.32         |  4.09        |  5.18             |  11.97      |  3.67          |  6.88            |  5.14
Oblivious  |  3.85       |  2.30         |  2.28        |  3.43             |  6.94       |  8.60          |  8.82            |  2.03
HDRF       |  3.90       |  2.12         |  2.18        |  3.33             |  7.27       |  7.90          |  8.87            |  1.62
Sheep      |  4.20       |  1.78         |  1.71        |  3.33             |  7.94       |  2.34          |  4.45            |  1.29
SNE        |  3.05       |  1.44         |  1.17        |  1.88             |  4.49       |  2.83          |  3.00            |  1.65
HSFC       |  3.80       |  3.75         |  2.66        |  3.78             |  6.31       |  5.82          |  4.80            |  1.96

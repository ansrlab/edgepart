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
*   A method via neighborhood heuristic (neighbor): our submission to NIPS'16

#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <random>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <parallel/algorithm>

#include "util.hpp"
#include "dense_bitset.hpp"
#include "edgepart.hpp"
#include "partitioner.hpp"

class RandomPartitioner : public Partitioner
{
  private:
    const size_t BUFFER_SIZE = 64 * 1024 / sizeof(edge_t);
    std::string basefilename;

    vid_t num_vertices;
    size_t num_edges;
    int p;

    // use mmap for file input
    int fin;
    off_t filesize;
    char *fin_map, *fin_ptr, *fin_end;

    std::random_device rd;
    std::mt19937 gen;

  public:
    RandomPartitioner(std::string basefilename);
    void split();
};

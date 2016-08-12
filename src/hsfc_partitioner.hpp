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

class HsfcPartitioner : public Partitioner
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

    uint64_t n;

    // convert (x,y) to d
    uint64_t xy2d(uint32_t x, uint32_t y)
    {
        uint64_t rx, ry, s, d = 0;
        for (s = n / 2; s > 0; s /= 2) {
            rx = (x & s) > 0;
            ry = (y & s) > 0;
            d += s * s * ((3 * rx) ^ ry);
            rot(s, &x, &y, rx, ry);
        }
        return d;
    }

    // convert d to (x,y)
    void d2xy(uint64_t d, uint32_t *x, uint32_t *y)
    {
        uint64_t rx, ry, s, t = d;
        *x = *y = 0;
        for (s = 1; s < n; s *= 2) {
            rx = 1 & (t / 2);
            ry = 1 & (t ^ rx);
            rot(s, x, y, rx, ry);
            *x += s * rx;
            *y += s * ry;
            t /= 4;
        }
    }

    // rotate/flip a quadrant appropriately
    void rot(uint64_t n, uint32_t *x, uint32_t *y, uint64_t rx, uint64_t ry)
    {
        if (ry == 0) {
            if (rx == 1) {
                *x = n - 1 - *x;
                *y = n - 1 - *y;
            }

            // Swap x and y
            uint32_t t = *x;
            *x = *y;
            *y = t;
        }
    }

    void generate_hilber();

  public:
    HsfcPartitioner(std::string basefilename);
    void split();
};

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

#include <boost/dynamic_bitset.hpp>

#include "util.hpp"
#include "min_heap.hpp"

class NeighborPartitioner
{
  private:
    typedef std::vector<std::vector<vid_t>> adjlist_t;

    std::string basefilename;

    vid_t num_vertices;
    size_t num_edges, assigned_edges;
    int p, bucket;
    double average_degree, local_average_degree;
    size_t sample_size, max_sample_size;
    size_t capacity, local_capacity;

    // use mmap for file input
    int fin;
    off_t filesize;
    char *fin_map, *fin_ptr, *fin_end;

    adjlist_t adj_out, adj_in;
    MinHeap<vid_t, vid_t> min_heap;
    std::vector<size_t> occupied;
    std::vector<vid_t> degrees;
    std::vector<boost::dynamic_bitset<>> is_cores, is_boundarys;

    std::random_device rd;
    std::mt19937 gen;
    std::uniform_int_distribution<vid_t> dis;

    bool check_edge(const edge_t *e)
    {
        rep (i, bucket) {
            if (is_boundarys[i][e->first] && is_boundarys[i][e->second] &&
                occupied[i] < capacity) {
                assign_edge(i, e->first, e->second);
                return false;
            }
        }

        rep (i, bucket) {
            if ((is_cores[i][e->first] || is_cores[i][e->second]) &&
                occupied[i] < capacity) {
                if (is_cores[i][e->first] && degrees[e->second] > average_degree)
                    continue;
                if (is_cores[i][e->second] && degrees[e->first] > average_degree)
                    continue;
                is_boundarys[i][e->first] = true;
                is_boundarys[i][e->second] = true;
                assign_edge(i, e->first, e->second);
                return false;
            }
        }

        return true;
    }

    void assign_edge(int bucket, vid_t from, vid_t to)
    {
        assigned_edges++;
        occupied[bucket]++;
        degrees[from]--;
        degrees[to]--;
    }

    void erase_one(std::vector<vid_t> &neighbors, const vid_t &v)
    {
        for (size_t i = 0; i < neighbors.size(); )
            if (neighbors[i] == v) {
                std::swap(neighbors[i], neighbors.back());
                neighbors.pop_back();
                return;
            } else
                i++;
        LOG(FATAL) << "reverse edge not found";
    }

    size_t erase(std::vector<vid_t> &neighbors, const vid_t &v)
    {
        size_t count = 0;
        for (size_t i = 0; i < neighbors.size(); )
            if (neighbors[i] == v) {
                std::swap(neighbors[i], neighbors.back());
                neighbors.pop_back();
                count++;
            } else
                i++;
        return count;
    }

    void add_boundary(vid_t vid)
    {
        boost::dynamic_bitset<> &is_core = is_cores[bucket];
        boost::dynamic_bitset<> &is_boundary = is_boundarys[bucket];

        if (is_boundary[vid])
            return;
        is_boundary[vid] = true;

        if (!is_core[vid]) {
            min_heap.insert(adj_out[vid].size() + adj_in[vid].size(), vid);
        }

        rep (direction, 2) {
            std::vector<vid_t> &adj = direction ? adj_out[vid] : adj_in[vid];
            adjlist_t &adj_r = direction ? adj_in : adj_out;
            for (size_t i = 0; i < adj.size();) {
                if (is_core[adj[i]]) {
                    sample_size--;
                    assign_edge(bucket, direction ? vid : adj[i], direction ? adj[i] : vid);
                    min_heap.decrease_key(vid);
                    std::swap(adj[i], adj.back());
                    adj.pop_back();
                } else if (is_boundary[adj[i]] && occupied[bucket] < local_capacity) {
                    sample_size--;
                    assign_edge(bucket, direction ? vid : adj[i], direction ? adj[i] : vid);
                    min_heap.decrease_key(vid);
                    erase_one(adj_r[adj[i]], vid);
                    min_heap.decrease_key(adj[i]);
                    std::swap(adj[i], adj.back());
                    adj.pop_back();
                } else
                    i++;
            }
        }
    }

    void occupy_vertex(vid_t vid, vid_t d)
    {
        CHECK(!is_cores[bucket][vid]) << "add " << vid << " to core again";
        is_cores[bucket][vid] = true;

        if (d == 0)
            return;

        add_boundary(vid);

        for (auto& w : adj_out[vid])
            add_boundary(w);
        adj_out[vid].clear();

        for (auto& w : adj_in[vid])
            add_boundary(w);
        adj_in[vid].clear();
    }

    bool get_free_vertex(vid_t &vid)
    {
        vid = dis(gen);
        int count = 0;
        while (count < num_vertices &&
               (adj_out[vid].size() + adj_in[vid].size() == 0 ||
                adj_out[vid].size() + adj_in[vid].size() >
                    2 * local_average_degree ||
                is_cores[bucket][vid])) {
            vid = (vid + ++count) % num_vertices;
        }
        if (count == num_vertices)
            return false;
        return true;
    }

    void read_more();
    void read_remaining();
    void clean_samples();
    size_t count_mirrors();

  public:
    NeighborPartitioner(std::string basefilename);
    void split();
};

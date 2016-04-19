#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <random>
#include <unordered_set>

#include <boost/container/flat_set.hpp>

#include "util.hpp"
#include "min_heap.hpp"

class NeighborPartitioner
{
  private:
    typedef std::vector<boost::container::flat_multiset<vid_t>> adjlist_t;

    vid_t num_vertices;
    size_t num_edges, assigned_edges;
    int p;
    double average_degree, local_average_degree;
    int bucket;
    size_t sample_size, max_sample_size;
    size_t capacity, local_capacity;
    std::vector<size_t> occupied;
    std::ifstream fin;
    std::string basefilename;
    std::vector<vid_t> degrees;
    adjlist_t adj_out, adj_in;
    std::vector<std::vector<bool>> is_cores, is_boundarys;
    MinHeap<vid_t, vid_t> min_heap;
    std::vector<std::unordered_set<vid_t>> num_mirrors;

    bool check_edge(const edge_t &e)
    {
        rep (i, bucket) {
            if (is_boundarys[i][e.first] && is_boundarys[i][e.second] &&
                occupied[i] < capacity) {
                assign_edge(i, e.first, e.second);
                return false;
            }
        }

        rep (i, bucket) {
            if ((is_cores[i][e.first] || is_cores[i][e.second]) &&
                occupied[i] < capacity) {
                if (is_cores[i][e.first] && degrees[e.second] > average_degree)
                    continue;
                if (is_cores[i][e.second] && degrees[e.first] > average_degree)
                    continue;
                is_boundarys[i][e.first] = true;
                is_boundarys[i][e.second] = true;
                assign_edge(i, e.first, e.second);
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
        num_mirrors[bucket].insert(from);
        num_mirrors[bucket].insert(to);
    }

    void add_boundary(vid_t vid) {
        std::vector<bool> &is_core = is_cores[bucket];
        std::vector<bool> &is_boundary = is_boundarys[bucket];
        if (is_boundary[vid])
            return;
        is_boundary[vid] = true;
        if (!is_core[vid]) {
            min_heap.insert(adj_out[vid].size() + adj_in[vid].size(), vid);
        }

        rep (direction, 2) {
            adjlist_t &adj = direction ? adj_out : adj_in,
                      &adj_r = direction ? adj_in : adj_out;
            for (auto it = adj[vid].begin(); it != adj[vid].end();) {
                if (is_core[*it]) {
                    sample_size--;
                    assign_edge(bucket, direction ? vid : *it, direction ? *it : vid);
                    min_heap.decrease_key(vid);
                    it = adj[vid].erase(it);
                } else if (is_boundary[*it]) {
                    sample_size--;
                    assign_edge(bucket, direction ? vid : *it, direction ? *it : vid);
                    min_heap.decrease_key(vid);
                    min_heap.decrease_key(*it, adj_r[*it].erase(vid));
                    it = adj[vid].erase(it);
                } else
                    it++;
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

        for (auto& w : adj_out[vid]) {
            add_boundary(w);
        }
        adj_out[vid].clear();

        for (auto& w : adj_in[vid]) {
            add_boundary(w);
        }
        adj_in[vid].clear();
    }

    void read_more();
    void read_remaining();

  public:
    NeighborPartitioner(std::string basefilename) : basefilename(basefilename)
    {
        LOG(INFO) << "initializing partitioner";
        p = FLAGS_p;
        fin.open(binedgelist_name(basefilename), std::ios::binary);
        fin.read((char *)&num_vertices, sizeof(num_vertices));
        fin.read((char *)&num_edges, sizeof(num_edges));
        LOG(INFO) << "num_vertices: " << num_vertices
                  << ", num_edges: " << num_edges;

        average_degree = (double)num_edges * 2 / num_vertices;
        assigned_edges = 0;
        max_sample_size = num_vertices * 2;
        local_average_degree = 2 * (double)max_sample_size / num_vertices;
        sample_size = 0;
        capacity = (double)num_edges * 1.05 / p + 1;
        occupied.assign(p, 0);
        num_mirrors.resize(p);
        adj_out.resize(num_vertices);
        adj_in.resize(num_vertices);
        is_cores.assign(p, std::vector<bool>(num_vertices));
        is_boundarys.assign(p, std::vector<bool>(num_vertices));

        degrees.resize(num_vertices);
        std::ifstream degree_file(degree_name(basefilename), std::ios::binary);
        degree_file.read((char *)&degrees[0], num_vertices * sizeof(vid_t));
        degree_file.close();
    };

    void split();
};

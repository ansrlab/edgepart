#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <random>
#include <unordered_set>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

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
    std::vector<std::vector<bool>> is_cores, is_boundarys;
    std::vector<std::unordered_set<vid_t>> num_mirrors;

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
        num_mirrors[bucket].insert(from);
        num_mirrors[bucket].insert(to);
    }

    size_t erase(std::vector<vid_t> &neighbors, vid_t v)
    {
        size_t count = 0;
        for (size_t i = 0; i < neighbors.size(); )
            if (neighbors[i] == v) {
                std::swap(neighbors[i], neighbors[neighbors.size() - 1]);
                neighbors.pop_back();
                count++;
            } else
                i++;
        return count;
    }

    void add_boundary(vid_t vid)
    {
        std::vector<bool> &is_core = is_cores[bucket];
        std::vector<bool> &is_boundary = is_boundarys[bucket];
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
                    std::swap(adj[i], adj[adj.size() - 1]);
                    adj.pop_back();
                } else if (is_boundary[adj[i]]) {
                    sample_size--;
                    assign_edge(bucket, direction ? vid : adj[i], direction ? adj[i] : vid);
                    min_heap.decrease_key(vid);
                    min_heap.decrease_key(adj[i], erase(adj_r[adj[i]], vid));
                    std::swap(adj[i], adj[adj.size() - 1]);
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

        fin = open(binedgelist_name(basefilename).c_str(), O_RDONLY,
                   (mode_t)0600);
        PCHECK(fin != -1) << "Error opening file for read";
        struct stat fileInfo = {0};
        PCHECK(fstat(fin, &fileInfo) != -1) << "Error getting the file size";
        PCHECK(fileInfo.st_size != 0) << "Error: file is empty";
        LOG(INFO) << "file size: " << fileInfo.st_size;

        fin_map = (char *)mmap(0, fileInfo.st_size, PROT_READ, MAP_SHARED, fin, 0);
        if (fin_map == MAP_FAILED) {
            close(fin);
            PLOG(FATAL) << "error mapping the file";
        }

        filesize = fileInfo.st_size;
        fin_ptr = fin_map;
        fin_end = fin_map + filesize;

        num_vertices = *(vid_t *)fin_ptr;
        fin_ptr += sizeof(vid_t);
        num_edges = *(size_t *)fin_ptr;
        fin_ptr += sizeof(size_t);

        LOG(INFO) << "num_vertices: " << num_vertices
                  << ", num_edges: " << num_edges;

        p = FLAGS_p;
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
        rep(i, num_vertices) {
            adj_out[i].reserve(local_average_degree);
            adj_in[i].reserve(local_average_degree);
        }
        is_cores.assign(p, std::vector<bool>(num_vertices));
        is_boundarys.assign(p, std::vector<bool>(num_vertices));

        degrees.resize(num_vertices);
        std::ifstream degree_file(degree_name(basefilename), std::ios::binary);
        degree_file.read((char *)&degrees[0], num_vertices * sizeof(vid_t));
        degree_file.close();
    };

    void split();
};

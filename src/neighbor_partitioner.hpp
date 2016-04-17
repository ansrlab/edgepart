#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <random>
#include <unordered_set>

#include <boost/container/flat_set.hpp>
#include <boost/unordered_map.hpp>

#include "util.hpp"

DECLARE_int32(p);

template<typename ValueType, typename KeyType, typename IdxType = vid_t>
class MinHeap {
private:
    IdxType n;
    std::vector<std::pair<ValueType, KeyType>> heap;
    boost::unordered_map<KeyType, IdxType> key2idx;

public:
    MinHeap() : n(0), heap(), key2idx() { }

    IdxType shift_up(IdxType cur) {
        if (cur == 0) return 0;
        IdxType p = (cur-1) / 2;

        if (heap[cur].first < heap[p].first) {
            std::swap(heap[cur], heap[p]);
            std::swap(key2idx[heap[cur].second], key2idx[heap[p].second]);
            return shift_up(p);
        }
        return cur;
    }

    void shift_down(IdxType cur) {
        IdxType l = cur*2 + 1;
        IdxType r = cur*2 + 2;

        if (l >= n)
            return;

        IdxType m = cur;
        if (heap[l].first < heap[cur].first)
            m = l;
        if (r < n && heap[r].first < heap[m].first)
            m = r;

        if (m != cur) {
            std::swap(heap[cur], heap[m]);
            std::swap(key2idx[heap[cur].second], key2idx[heap[m].second]);
            shift_down(m);
        }
    }

    void insert(ValueType value, KeyType key) {
        heap[n] = std::make_pair(value, key);
        key2idx[key] = n++;
        IdxType cur = shift_up(n-1);
        shift_down(cur);
    }

    bool contains(KeyType key) {
        return key2idx.count(key);
    }

    void decrease_key(KeyType key, ValueType d = 1) {
        if (d == 0) return;
        auto it = key2idx.find(key);
        CHECK(it != key2idx.end()) << "key not found";
        IdxType cur = it->second;

        CHECK_GE(heap[cur].first, d) << "value cannot be negative";
        heap[cur].first -= d;
        shift_up(cur);
    }

    bool remove(KeyType key) {
        auto it = key2idx.find(key);
        if (it == key2idx.end())
            return false;
        IdxType cur = it->second;

        n--;
        if (n > 0) {
            heap[cur] = heap[n];
            key2idx[heap[cur].second] = cur;
            cur = shift_up(cur);
            shift_down(cur);
        }
        key2idx.erase(it);
        return true;
    }

    bool get_min(ValueType& value, KeyType& key) {
        if (n > 0) {
            value = heap[0].first;
            key = heap[0].second;
            return true;
        } else
            return false;
    }

    void reset(IdxType nelements) {
        n = 0;
        heap.resize(nelements);
        key2idx.clear();
    }

    void clear() {
        n = 0;
        heap.clear();
        key2idx.clear();
    }
};

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

    void read_more()
    {
        while (sample_size < max_sample_size && !fin.eof()) {
            edge_t e;
            fin.read((char *)&e, sizeof(edge_t));
            if (fin.eof())
                return;
            if (check_edge(e)) {
                sample_size++;
                adj_out[e.first].insert(e.second);
                adj_in[e.second].insert(e.first);
            }
        }
    }

    void read_remaining()
    {
        rep (u, num_vertices)
            for (auto &v : adj_out[u])
                assign_edge(p - 1, u, v);

        while (!fin.eof()) {
            edge_t e;
            fin.read((char *)&e, sizeof(edge_t));
            if (fin.eof())
                return;
            if (check_edge(e)) {
                assign_edge(p - 1, e.first, e.second);
            }
        }
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

    void split()
    {
        LOG(INFO) << "partition `" << basefilename << "'";
        LOG(INFO) << "number of partitions: " << p;

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<vid_t> dis(0, num_vertices-1);

        min_heap.reset(num_vertices);
        for (bucket = 0; bucket < p - 1; bucket++) {
            DLOG(INFO) << "start partition " << bucket;
            read_more();
            DLOG(INFO) << "sample size: " << sample_size;
            local_capacity = sample_size / (p - bucket);
            while (occupied[bucket] < local_capacity) {
                vid_t d, vid;
                if (!min_heap.get_min(d, vid)) {
                    vid = dis(gen);
                    int count = 0;
                    while (count < num_vertices &&
                           (adj_out[vid].size() + adj_in[vid].size() == 0 ||
                            adj_out[vid].size() + adj_in[vid].size() > 2 * local_average_degree ||
                            is_cores[bucket][vid])) {
                        vid = (vid + ++count) % num_vertices;
                    }
                    if (count == num_vertices) {
                        DLOG(INFO) << "not free vertices";
                        break;
                    }
                    d = adj_out[vid].size() + adj_in[vid].size();
                } else {
                    min_heap.remove(vid);
                    CHECK_EQ(d, adj_out[vid].size() + adj_in[vid].size());
                }

                occupy_vertex(vid, d);
            }
            min_heap.clear();
        }
        DLOG(INFO) << "last partition";
        bucket = p - 1;
        read_remaining();
        fin.close();
        rep (i, p)
            DLOG(INFO) << "edges in partition " << i << ": " << occupied[i];
        size_t max_occupied = *std::max_element(occupied.begin(), occupied.end());
        LOG(INFO) << "balance: " << (double)max_occupied / ((double) num_edges / p);
        size_t total_mirrors = 0;
        rep (i, p)
            total_mirrors += num_mirrors[i].size();
        LOG(INFO) << "total mirrors: " << total_mirrors;
        LOG(INFO) << "replication factor: "
                  << (double)total_mirrors / num_vertices;
        CHECK_EQ(assigned_edges, num_edges);
    }
};

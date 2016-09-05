#pragma once

#include <vector>
#include <parallel/algorithm>

#include "util.hpp"

class adjlist_t
{
  private:
    vid_t *adj;
    vid_t len;

  public:
    adjlist_t() : adj(NULL), len(0) {}
    adjlist_t(vid_t *adj, vid_t len) : adj(adj), len(len) {}
    vid_t *begin() { return adj; }
    vid_t *end() { return adj + len; }
    void increment() { len++; }
    size_t size() const { return len; }
    vid_t &operator[](size_t idx) { return adj[idx]; };
    const vid_t &operator[](size_t idx) const { return adj[idx]; };
    vid_t &back() { return adj[len - 1]; };
    const vid_t &back() const { return adj[len - 1]; };
    void pop_back() { len--; }
    void clear() { len = 0; }
};

class graph_t
{
  private:
    vid_t num_vertices;
    std::vector<vid_t> neighbors;
    std::vector<adjlist_t> vdata;

  public:
    void resize(vid_t _num_vertices)
    {
        num_vertices = _num_vertices;
        vdata.resize(num_vertices);
    }

    size_t num_edges() const { return neighbors.size(); }

    void build(std::vector<edge_t> &edges);

    void build_reverse(std::vector<edge_t> &edges);

    adjlist_t &operator[](size_t idx) { return vdata[idx]; };
    const adjlist_t &operator[](size_t idx) const { return vdata[idx]; };
};

void erase_one(adjlist_t &neighbors, const vid_t &v);

size_t erase(adjlist_t &neighbors, const vid_t &v);

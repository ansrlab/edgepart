#include "graph.hpp"

void graph_t::build(std::vector<edge_t> &edges)
{
    repv (v, num_vertices)
        vdata[v].clear();
    __gnu_parallel::sort(edges.begin(), edges.end(),
                         [](const edge_t &a, const edge_t &b) {
                             return a.first < b.first;
                         });
    neighbors.resize(edges.size());
    vid_t last_v = -1;
    vid_t *start_ptr = &neighbors[0];
    for (size_t i = 0; i < edges.size(); i++) {
        neighbors[i] = edges[i].second;
        if (edges[i].first == last_v) {
            vdata[last_v].increment();
        } else {
            vdata[edges[i].first] = adjlist_t(start_ptr + i, 1);
            last_v = edges[i].first;
        }
    }
}

void graph_t::build_reverse(std::vector<edge_t> &edges)
{
    repv (v, num_vertices)
        vdata[v].clear();
    __gnu_parallel::sort(edges.begin(), edges.end(),
                         [](const edge_t &a, const edge_t &b) {
                             return a.second < b.second;
                         });
    neighbors.resize(edges.size());
    vid_t last_v = -1;
    vid_t *start_ptr = &neighbors[0];
    for (size_t i = 0; i < edges.size(); i++) {
        neighbors[i] = edges[i].first;
        if (edges[i].second == last_v) {
            vdata[last_v].increment();
        } else {
            vdata[edges[i].second] = adjlist_t(start_ptr + i, 1);
            last_v = edges[i].second;
        }
    }
}

void erase_one(adjlist_t &neighbors, const vid_t &v)
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

size_t erase(adjlist_t &neighbors, const vid_t &v)
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

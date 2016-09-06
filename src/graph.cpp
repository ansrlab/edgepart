#include "graph.hpp"

void graph_t::build(const std::vector<edge_t> &edges)
{
    std::vector<size_t> count(num_vertices, 0);
    for (size_t i = 0; i < edges.size(); i++)
        count[edges[i].first]++;
    neighbors.resize(edges.size());
    vdata[0] = adjlist_t(&neighbors[0]);
    for (vid_t v = 1; v < num_vertices; v++) {
        count[v] += count[v-1];
        vdata[v] = adjlist_t(&neighbors[count[v-1]]);
    }
    for (size_t i = 0; i < edges.size(); i++)
        vdata[edges[i].first].push_back(i);
}

void graph_t::build_reverse(const std::vector<edge_t> &edges)
{
    std::vector<size_t> count(num_vertices, 0);
    for (size_t i = 0; i < edges.size(); i++)
        count[edges[i].second]++;
    neighbors.resize(edges.size());
    vdata[0] = adjlist_t(&neighbors[0]);
    for (vid_t v = 1; v < num_vertices; v++) {
        count[v] += count[v-1];
        vdata[v] = adjlist_t(&neighbors[count[v-1]]);
    }
    for (size_t i = 0; i < edges.size(); i++)
        vdata[edges[i].second].push_back(i);
}

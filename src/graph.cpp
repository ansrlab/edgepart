#include "graph.hpp"

void graph_t::build(const std::vector<edge_t> &edges)
{
    std::vector<size_t> count(num_vertices, 0);
    for (size_t i = 0; i < edges.size(); i++)
        count[edges[i].first]++;
    if (neighbors == NULL)
        neighbors = (size_t *)malloc(sizeof(size_t) * edges.size());
    else if (edges.size() > nedges)
        neighbors = (size_t *)realloc(neighbors, sizeof(size_t) * edges.size());
    CHECK(neighbors) << "allocation failed";
    nedges = edges.size();
    vdata[0] = adjlist_t(neighbors);
    for (vid_t v = 1; v < num_vertices; v++) {
        count[v] += count[v-1];
        vdata[v] = adjlist_t(neighbors + count[v-1]);
    }
    for (size_t i = 0; i < edges.size(); i++)
        vdata[edges[i].first].push_back(i);
}

void graph_t::build_reverse(const std::vector<edge_t> &edges)
{
    std::vector<size_t> count(num_vertices, 0);
    for (size_t i = 0; i < edges.size(); i++)
        count[edges[i].second]++;
    if (neighbors == NULL)
        neighbors = (size_t *)malloc(sizeof(size_t) * edges.size());
    else if (edges.size() > nedges)
        neighbors = (size_t *)realloc(neighbors, sizeof(size_t) * edges.size());
    CHECK(neighbors) << "allocation failed";
    nedges = edges.size();
    vdata[0] = adjlist_t(neighbors);
    for (vid_t v = 1; v < num_vertices; v++) {
        count[v] += count[v-1];
        vdata[v] = adjlist_t(neighbors + count[v-1]);
    }
    for (size_t i = 0; i < edges.size(); i++)
        vdata[edges[i].second].push_back(i);
}

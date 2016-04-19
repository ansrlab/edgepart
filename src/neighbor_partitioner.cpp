#include "neighbor_partitioner.hpp"

void NeighborPartitioner::read_more()
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

void NeighborPartitioner::read_remaining()
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

void NeighborPartitioner::split()
{
    LOG(INFO) << "partition `" << basefilename << "'";
    LOG(INFO) << "number of partitions: " << p;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<vid_t> dis(0, num_vertices - 1);

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
                        adj_out[vid].size() + adj_in[vid].size() >
                            2 * local_average_degree ||
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
    LOG(INFO) << "balance: " << (double)max_occupied / ((double)num_edges / p);
    size_t total_mirrors = 0;
    rep (i, p)
        total_mirrors += num_mirrors[i].size();
    LOG(INFO) << "total mirrors: " << total_mirrors;
    LOG(INFO) << "replication factor: " << (double)total_mirrors / num_vertices;
    LOG_IF(WARNING, assigned_edges != num_edges)
        << "assigned_edges != num_edges: " << assigned_edges << " vs. "
        << num_edges;
}

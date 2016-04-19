#include "neighbor_partitioner.hpp"

void NeighborPartitioner::read_more()
{
    while (sample_size < max_sample_size && fin_ptr < fin_end) {
        edge_t *e = (edge_t *)fin_ptr;
        fin_ptr += sizeof(edge_t);
        if (check_edge(e)) {
            sample_size++;
            adj_out[e->first].push_back(e->second);
            adj_in[e->second].push_back(e->first);
        }
    }
}

void NeighborPartitioner::read_remaining()
{
    rep (u, num_vertices)
        for (auto &v : adj_out[u]) {
            is_boundarys[p - 1][u] = true;
            is_boundarys[p - 1][v] = true;
            assign_edge(p - 1, u, v);
        }

    while (fin_ptr < fin_end) {
        edge_t *e = (edge_t *)fin_ptr;
        fin_ptr += sizeof(edge_t);
        if (check_edge(e)) {
            is_boundarys[p - 1][e->first] = true;
            is_boundarys[p - 1][e->second] = true;
            assign_edge(p - 1, e->first, e->second);
        }
    }

    rep (i, num_vertices) {
        if (is_boundarys[p - 1][i]) {
            is_cores[p - 1][i] = true;
            rep (j, p - 1)
                if (is_cores[j][i]) {
                    is_cores[p - 1][i] = false;
                    break;
                }
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

    Timer read_timer, compute_timer;

    min_heap.reserve(num_vertices);
    for (bucket = 0; bucket < p - 1; bucket++) {
        DLOG(INFO) << "start partition " << bucket;
        read_timer.start();
        read_more();
        read_timer.stop();
        DLOG(INFO) << "sample size: " << sample_size;
        compute_timer.start();
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
        compute_timer.stop();
    }
    DLOG(INFO) << "last partition";
    bucket = p - 1;
    read_timer.start();
    read_remaining();
    read_timer.stop();
    rep (i, p)
        DLOG(INFO) << "edges in partition " << i << ": " << occupied[i];
    size_t max_occupied = *std::max_element(occupied.begin(), occupied.end());
    LOG(INFO) << "balance: " << (double)max_occupied / ((double)num_edges / p);
    size_t total_mirrors = 0;
    rep (i, p)
        total_mirrors += is_boundarys[i].count();
    LOG(INFO) << "total mirrors: " << total_mirrors;
    LOG(INFO) << "replication factor: " << (double)total_mirrors / num_vertices;
    LOG(INFO) << "time used for graph input and construction: " << read_timer.get_time();
    LOG(INFO) << "time used for partitioning: " << compute_timer.get_time();
    LOG_IF(WARNING, assigned_edges != num_edges)
        << "assigned_edges != num_edges: " << assigned_edges << " vs. "
        << num_edges;

    if (munmap(fin_map, filesize) == -1) {
        close(fin);
        PLOG(FATAL) << "Error un-mmapping the file";
    }
    close(fin);
}

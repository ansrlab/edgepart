#include "neighbor_partitioner.hpp"

NeighborPartitioner::NeighborPartitioner(std::string basefilename)
    : basefilename(basefilename), gen(rd())
{
    LOG(INFO) << "initializing partitioner";

    fin = open(binedgelist_name(basefilename).c_str(), O_RDONLY, (mode_t)0600);
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
    adj_out.resize(num_vertices);
    adj_in.resize(num_vertices);
    rep (i, num_vertices) {
        adj_out[i].reserve(local_average_degree);
        adj_in[i].reserve(local_average_degree);
    }
    is_cores.assign(p, boost::dynamic_bitset<>(num_vertices));
    is_boundarys.assign(p, boost::dynamic_bitset<>(num_vertices));
    dis.param(std::uniform_int_distribution<vid_t>::param_type(0, num_vertices - 1));

    degrees.resize(num_vertices);
    std::ifstream degree_file(degree_name(basefilename), std::ios::binary);
    degree_file.read((char *)&degrees[0], num_vertices * sizeof(vid_t));
    degree_file.close();
};

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
    boost::dynamic_bitset<> &is_boundary = is_boundarys[p - 1];
    boost::dynamic_bitset<> &is_core = is_cores[p - 1];

    rep (u, num_vertices)
        for (auto &v : adj_out[u]) {
            is_boundary[u] = true;
            is_boundary[v] = true;
            assign_edge(p - 1, u, v);
        }

    while (fin_ptr < fin_end) {
        edge_t *e = (edge_t *)fin_ptr;
        fin_ptr += sizeof(edge_t);
        if (check_edge(e)) {
            is_boundary[e->first] = true;
            is_boundary[e->second] = true;
            assign_edge(p - 1, e->first, e->second);
        }
    }

    rep (i, num_vertices) {
        if (is_boundary[i]) {
            is_core[i] = true;
            rep (j, p - 1)
                if (is_cores[j][i]) {
                    is_core[i] = false;
                    break;
                }
        }
    }
}

size_t NeighborPartitioner::count_mirrors()
{
    size_t result = 0;
    rep (i, p)
        result += is_boundarys[i].count();
    return result;
}

void NeighborPartitioner::split()
{
    LOG(INFO) << "partition `" << basefilename << "'";
    LOG(INFO) << "number of partitions: " << p;

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
                if (!get_free_vertex(vid)) {
                    LOG(INFO) << "partition " << bucket
                              << " stop: no free vertices";
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
    bucket = p - 1;
    DLOG(INFO) << "start partition " << bucket;
    read_timer.start();
    read_remaining();
    read_timer.stop();
    LOG(INFO) << "expected edges in each partition: " << num_edges / p;
    rep (i, p)
        DLOG(INFO) << "edges in partition " << i << ": " << occupied[i];
    size_t max_occupied = *std::max_element(occupied.begin(), occupied.end());
    LOG(INFO) << "balance: " << (double)max_occupied / ((double)num_edges / p);
    size_t total_mirrors = count_mirrors();
    LOG(INFO) << "total mirrors: " << total_mirrors;
    LOG(INFO) << "replication factor: " << (double)total_mirrors / num_vertices;
    LOG(INFO) << "time used for graph input and construction: " << read_timer.get_time();
    LOG(INFO) << "time used for partitioning: " << compute_timer.get_time();

    if (munmap(fin_map, filesize) == -1) {
        close(fin);
        PLOG(FATAL) << "Error un-mmapping the file";
    }
    close(fin);

    CHECK_EQ(assigned_edges, num_edges);
}

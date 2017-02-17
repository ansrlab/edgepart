#include "ne_partitioner.hpp"
#include "conversions.hpp"

NePartitioner::NePartitioner(std::string basefilename)
    : basefilename(basefilename), rd(), gen(rd()), writer(basefilename)
{
    Timer convert_timer;
    convert_timer.start();
    Converter *converter = new Converter(basefilename);
    convert(basefilename, converter);
    delete converter;
    convert_timer.stop();
    LOG(INFO) << "convert time: " << convert_timer.get_time();

    total_time.start();
    LOG(INFO) << "initializing partitioner";

    std::ifstream fin(binedgelist_name(basefilename),
                      std::ios::binary | std::ios::ate);
    auto filesize = fin.tellg();
    LOG(INFO) << "file size: " << filesize;
    fin.seekg(0, std::ios::beg);

    fin.read((char *)&num_vertices, sizeof(num_vertices));
    fin.read((char *)&num_edges, sizeof(num_edges));

    LOG(INFO) << "num_vertices: " << num_vertices
              << ", num_edges: " << num_edges;
    CHECK_EQ(sizeof(vid_t) + sizeof(size_t) + num_edges * sizeof(edge_t), filesize);

    p = FLAGS_p;
    average_degree = (double)num_edges * 2 / num_vertices;
    assigned_edges = 0;
    capacity = (double)num_edges * BALANCE_RATIO / p + 1;
    occupied.assign(p, 0);
    adj_out.resize(num_vertices);
    adj_in.resize(num_vertices);
    is_cores.assign(p, dense_bitset(num_vertices));
    is_boundarys.assign(p, dense_bitset(num_vertices));
    master.assign(num_vertices, -1);
    dis.param(std::uniform_int_distribution<vid_t>::param_type(0, num_vertices - 1));

    Timer read_timer;
    read_timer.start();
    LOG(INFO) << "loading...";
    edges.resize(num_edges);
    fin.read((char *)&edges[0], sizeof(edge_t) * num_edges);

    LOG(INFO) << "constructing...";
    adj_out.build(edges);
    adj_in.build_reverse(edges);

    degrees.resize(num_vertices);
    std::ifstream degree_file(degree_name(basefilename), std::ios::binary);
    degree_file.read((char *)&degrees[0], num_vertices * sizeof(vid_t));
    degree_file.close();
    read_timer.stop();
    LOG(INFO) << "time used for graph input and construction: " << read_timer.get_time();
};

void NePartitioner::assign_remaining()
{
    auto &is_boundary = is_boundarys[p - 1], &is_core = is_cores[p - 1];
    repv (u, num_vertices)
        for (auto &i : adj_out[u])
            if (edges[i.v].valid()) {
                assign_edge(p - 1, u, edges[i.v].second);
                is_boundary.set_bit_unsync(u);
                is_boundary.set_bit_unsync(edges[i.v].second);
            }

    repv (i, num_vertices) {
        if (is_boundary.get(i)) {
            is_core.set_bit_unsync(i);
            rep (j, p - 1)
                if (is_cores[j].get(i)) {
                    is_core.set_unsync(i, false);
                    break;
                }
        }
    }
}

void NePartitioner::assign_master()
{
    std::vector<vid_t> count_master(p, 0);
    std::vector<vid_t> quota(p, num_vertices);
    long long sum = p * num_vertices;
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    std::vector<dense_bitset::iterator> pos(p);
    rep (b, p)
        pos[b] = is_boundarys[b].begin();
    vid_t count = 0;
    while (count < num_vertices) {
        long long r = distribution(gen) * sum;
        int k;
        for (k = 0; k < p; k++) {
            if (r < quota[k])
                break;
            r -= quota[k];
        }
        while (pos[k] != is_boundarys[k].end() && master[*pos[k]] != -1)
            pos[k]++;
        if (pos[k] != is_boundarys[k].end()) {
            count++;
            master[*pos[k]] = k;
            writer.save_vertex(*pos[k], k);
            count_master[k]++;
            quota[k]--;
            sum--;
        }
    }
    int max_masters =
        *std::max_element(count_master.begin(), count_master.end());
    LOG(INFO) << "master balance: "
              << (double)max_masters / ((double)num_vertices / p);
}

size_t NePartitioner::count_mirrors()
{
    size_t result = 0;
    rep (i, p)
        result += is_boundarys[i].popcount();
    return result;
}

void NePartitioner::split()
{
    LOG(INFO) << "partition `" << basefilename << "'";
    LOG(INFO) << "number of partitions: " << p;

    Timer compute_timer;

    min_heap.reserve(num_vertices);

    LOG(INFO) << "partitioning...";
    compute_timer.start();
    for (bucket = 0; bucket < p - 1; bucket++) {
        std::cerr << bucket << ", ";
        DLOG(INFO) << "sample size: " << adj_out.num_edges();
        while (occupied[bucket] < capacity) {
            vid_t d, vid;
            if (!min_heap.get_min(d, vid)) {
                if (!get_free_vertex(vid)) {
                    DLOG(INFO) << "partition " << bucket
                               << " stop: no free vertices";
                    break;
                }
                d = adj_out[vid].size() + adj_in[vid].size();
            } else {
                min_heap.remove(vid);
                /* CHECK_EQ(d, adj_out[vid].size() + adj_in[vid].size()); */
            }

            occupy_vertex(vid, d);
        }
        min_heap.clear();
        rep (direction, 2)
            repv (vid, num_vertices) {
                adjlist_t &neighbors = direction ? adj_out[vid] : adj_in[vid];
                for (size_t i = 0; i < neighbors.size();) {
                    if (edges[neighbors[i].v].valid()) {
                        i++;
                    } else {
                        std::swap(neighbors[i], neighbors.back());
                        neighbors.pop_back();
                    }
                }
            }
    }
    bucket = p - 1;
    std::cerr << bucket << std::endl;
    assign_remaining();
    assign_master();
    compute_timer.stop();
    LOG(INFO) << "expected edges in each partition: " << num_edges / p;
    rep (i, p)
        DLOG(INFO) << "edges in partition " << i << ": " << occupied[i];
    size_t max_occupied = *std::max_element(occupied.begin(), occupied.end());
    LOG(INFO) << "balance: " << (double)max_occupied / ((double)num_edges / p);
    size_t total_mirrors = count_mirrors();
    LOG(INFO) << "total mirrors: " << total_mirrors;
    LOG(INFO) << "replication factor: " << (double)total_mirrors / num_vertices;
    LOG(INFO) << "time used for partitioning: " << compute_timer.get_time();

    CHECK_EQ(assigned_edges, num_edges);

    total_time.stop();
    LOG(INFO) << "total partition time: " << total_time.get_time();
}

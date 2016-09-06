#include "ne_partitioner.hpp"
#include "conversions.hpp"

NePartitioner::NePartitioner(std::string basefilename)
    : basefilename(basefilename), rd(), gen(rd()), writer(basefilename)
{
    Timer convert_timer;
    convert_timer.start();
    convert(basefilename, new Converter(basefilename));
    convert_timer.stop();
    LOG(INFO) << "convert time: " << convert_timer.get_time();

    total_time.start();
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
    dis.param(std::uniform_int_distribution<vid_t>::param_type(0, num_vertices - 1));

    degrees.resize(num_vertices);
    std::ifstream degree_file(degree_name(basefilename), std::ios::binary);
    degree_file.read((char *)&degrees[0], num_vertices * sizeof(vid_t));
    degree_file.close();
};

void NePartitioner::load_graph()
{
    LOG(INFO) << "loading...";
    while (fin_ptr < fin_end) {
        const edge_t *e = (edge_t *)fin_ptr;
        edges.push_back(*e);
        fin_ptr += sizeof(edge_t);
    }

    LOG(INFO) << "constructing...";
    adj_out.build(edges);
    adj_in.build_reverse(edges);
}

void NePartitioner::assign_remaining()
{
    repv (u, num_vertices)
        for (auto &i : adj_out[u])
            if (edges[i].valid())
                assign_edge(p - 1, u, edges[i].second);
}

void NePartitioner::assign_master()
{
    std::vector<vid_t> count_master(p, 0);
    std::vector<bool> allocated(num_vertices);
    rep (i, p-1) {
        auto is_core = is_cores[i];
        repv (v, num_vertices)
            if (is_core.get(v)) {
                count_master[i]++;
                allocated[v] = true;
                writer.save_vertex(v, i);
            }
    }
    repv (v, num_vertices)
        if (!allocated[v]) {
            count_master[p-1]++;
            writer.save_vertex(v, p-1);
        }
    int max_masters =
        *std::max_element(count_master.begin(), count_master.end());
    vid_t total_master = std::accumulate(count_master.begin(), count_master.end(), 0);
    CHECK_EQ(total_master, num_vertices);
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

    Timer read_timer, compute_timer;

    min_heap.reserve(num_vertices);
    edges.reserve(num_edges);

    read_timer.start();
    load_graph();
    read_timer.stop();

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
                    if (edges[neighbors[i]].valid()) {
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
    LOG(INFO) << "time used for graph input and construction: " << read_timer.get_time();
    LOG(INFO) << "time used for partitioning: " << compute_timer.get_time();

    if (munmap(fin_map, filesize) == -1) {
        close(fin);
        PLOG(FATAL) << "Error un-mmapping the file";
    }
    close(fin);

    CHECK_EQ(assigned_edges, num_edges);

    total_time.stop();
    LOG(INFO) << "total partition time: " << total_time.get_time();
}

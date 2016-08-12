#include <utility>

#include "util.hpp"
#include "dbh_partitioner.hpp"
#include "conversions.hpp"

DbhPartitioner::DbhPartitioner(std::string basefilename)
{
    if (!is_exists(binedgelist_name(basefilename))) {
        Timer convert_timer;
        convert_timer.start();
        convert(basefilename, new Converter(basefilename));
        convert_timer.stop();
        LOG(INFO) << "convert time: " << convert_timer.get_time();
    } else
        LOG(INFO) << "skip conversion";

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

    p = FLAGS_p;
    dis.param(std::uniform_int_distribution<int>::param_type(0, p - 1));

    degrees.resize(num_vertices);
    std::ifstream degree_file(degree_name(basefilename), std::ios::binary);
    degree_file.read((char *)&degrees[0], num_vertices * sizeof(vid_t));
    degree_file.close();
}

void DbhPartitioner::split()
{
    std::vector<dense_bitset> is_mirrors(p, dense_bitset(num_vertices));
    std::vector<size_t> counter(p, 0);
    while (fin_ptr < fin_end) {
        edge_t *e = (edge_t *)fin_ptr;
        fin_ptr += sizeof(edge_t);
        vid_t w = degrees[e->first] <= degrees[e->second] ? e->first : e->second;
        int bucket = w % p;
        counter[bucket]++;
        is_mirrors[bucket].set_bit_unsync(e->first);
        is_mirrors[bucket].set_bit_unsync(e->second);
    }

    if (munmap(fin_map, filesize) == -1) {
        close(fin);
        PLOG(FATAL) << "Error un-mmapping the file";
    }
    close(fin);


    size_t max_occupied = *std::max_element(counter.begin(), counter.end());
    LOG(INFO) << "balance: " << (double)max_occupied / ((double)num_edges / p);
    size_t total_mirrors = 0;
    rep (i, p)
        total_mirrors += is_mirrors[i].popcount();
    LOG(INFO) << "total mirrors: " << total_mirrors;
    LOG(INFO) << "replication factor: " << (double)total_mirrors / num_vertices;

    total_time.stop();
    LOG(INFO) << "total partition time: " << total_time.get_time();
}

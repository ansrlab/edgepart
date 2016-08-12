#include <utility>
#include <functional>
#include <fcntl.h>

#include "util.hpp"
#include "hsfc_partitioner.hpp"
#include "sort.hpp"
#include "conversions.hpp"

HsfcPartitioner::HsfcPartitioner(std::string basefilename)
    : basefilename(basefilename)
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

    n = 1;
    while (n < num_vertices)
        n = n << 1;

    p = FLAGS_p;
}

void HsfcPartitioner::generate_hilber()
{
    Timer timer;
    timer.start();
    LOG(INFO) << "generating hilber distance file...";
    std::ofstream fout(hilbert_name(basefilename), std::ios::binary);
    int gb = 0;
    size_t bytes = 0;
    while (fin_ptr < fin_end) {
        edge_t *e = (edge_t *)fin_ptr;
        fin_ptr += sizeof(edge_t);
        bytes += sizeof(edge_t);
        vid_t u = e->first, v = e->second;
        uint64_t d = xy2d(u, v);
        fout.write((char *)&d, sizeof(d));
        if (bytes == 1024*1024*1024) {
            gb++;
            bytes = 0;
        }
    }
    fout.close();
    timer.stop();
    LOG(INFO) << "load time: " << timer.get_time();
}

void HsfcPartitioner::split()
{
    std::vector<dense_bitset> is_mirrors(p, dense_bitset(num_vertices));
    std::vector<size_t> counter(p, 0);

    if (!is_exists(hilbert_name(basefilename)))
        generate_hilber();
    else
        LOG(INFO) << "skip generating hilbert distance file";
    if (munmap(fin_map, filesize) == -1) {
        close(fin);
        PLOG(FATAL) << "Error un-mmapping the file";
    }
    close(fin);

    Timer timer;
    if (!is_exists(sorted_hilbert_name(basefilename))) {
        timer.start();
        LOG(INFO) << "sorting...";
        int fdInput = open(hilbert_name(basefilename).c_str(), O_RDONLY);
        PCHECK(fdInput != -1) << "Error opening file for read";
        int fdOutput = open(sorted_hilbert_name(basefilename).c_str(),
                            O_WRONLY | O_CREAT | O_TRUNC,
                            S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        PCHECK(fdOutput != -1) << "Error opening file `"
                               << sorted_hilbert_name(basefilename)
                               << "` for write";
        externalSort(fdInput, num_edges, fdOutput, FLAGS_memsize * 1024 * 1024);
        close(fdInput);
        close(fdOutput);
        timer.stop();
        LOG(INFO) << "sort time: " << timer.get_time();
    } else
        LOG(INFO) << "skip sorting";

    timer.reset();
    timer.start();
    LOG(INFO) << "partitioning...";
    std::ifstream hilbert_file(sorted_hilbert_name(basefilename), std::ios::binary);
    size_t range = num_edges / p + 1;
    for (size_t i = 0; i < num_edges; i++) {
        int bucket = i / range;
        counter[bucket]++;
        vid_t u, v;
        uint64_t d;
        hilbert_file.read((char *)&d, sizeof(d));
        d2xy(d, &u, &v);
        is_mirrors[bucket].set_bit_unsync(u);
        is_mirrors[bucket].set_bit_unsync(v);
    }
    timer.stop();
    LOG(INFO) << "partition time: " << timer.get_time();

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

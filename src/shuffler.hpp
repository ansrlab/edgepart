#pragma once

#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "util.hpp"
#include "conversions.hpp"

class Shuffler : public Converter
{
  private:
    struct work_t {
        Shuffler *shuffler;
        int nchunks;
        std::vector<edge_t> chunk_buf;

        void operator()() {
            std::random_shuffle(chunk_buf.begin(), chunk_buf.end());
            int file =
                open(shuffler->chunk_filename(nchunks).c_str(),
                     O_WRONLY | O_CREAT, S_IROTH | S_IWOTH | S_IWUSR | S_IRUSR);
            size_t chunk_size = chunk_buf.size() * sizeof(edge_t);
            writea(file, (char *)&chunk_buf[0], chunk_size);
            close(file);
            chunk_buf.clear();
        }
    };

    size_t chunk_bufsize;
    int nchunks, old_nchunks;
    std::vector<edge_t> chunk_buf, old_chunk_buf;

    std::string chunk_filename(int chunk);
    void chunk_clean();
    void cwrite(edge_t e, bool flush = false);

  public:
    Shuffler(std::string basefilename) : Converter(basefilename) {}
    bool done() { return is_exists(shuffled_binedgelist_name(basefilename)); }
    void init();
    void finalize();
    void add_edge(vid_t source, vid_t target);
};

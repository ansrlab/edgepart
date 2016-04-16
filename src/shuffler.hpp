#pragma once

#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <boost/unordered_map.hpp>

#include "util.hpp"

class Shuffler
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

    std::string basefilename;
    vid_t num_vertices;
    size_t num_edges;
    size_t chunk_bufsize;
    int nchunks, old_nchunks;
    std::vector<vid_t> degrees;
    std::vector<edge_t> chunk_buf, old_chunk_buf;
    boost::unordered_map<vid_t, vid_t> name2vid;

    std::string chunk_filename(int chunk);
    void chunk_clean();
    void cwrite(edge_t e, bool flush = false);
    vid_t get_vid(vid_t v)
    {
        auto it = name2vid.find(v);
        if (it == name2vid.end()) {
            name2vid[v] = num_vertices;
            degrees.resize(num_vertices + 1);
            return num_vertices++;
        }
        return name2vid[v];
    }

  public:
    Shuffler(std::string basefilename) : basefilename(basefilename) {}
    void init();
    void finalize();
    void add_edge(vid_t source, vid_t target);
};

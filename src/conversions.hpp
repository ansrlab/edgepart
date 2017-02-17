#pragma once

#include <string>
#include <fstream>

#include <boost/unordered_map.hpp>

#include "util.hpp"

DECLARE_string(filetype);

class Converter
{
  protected:
    std::string basefilename;
    vid_t num_vertices;
    size_t num_edges;
    std::vector<vid_t> degrees;
    std::ofstream fout;
    boost::unordered_map<vid_t, vid_t> name2vid;

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
    Converter(std::string basefilename) : basefilename(basefilename) {}
    virtual ~Converter() {}
    virtual bool done() { return is_exists(binedgelist_name(basefilename)); }

    virtual void init()
    {
        num_vertices = 0;
        num_edges = 0;
        degrees.reserve(1<<20);
        fout.open(binedgelist_name(basefilename), std::ios::binary);
        fout.write((char *)&num_vertices, sizeof(num_vertices));
        fout.write((char *)&num_edges, sizeof(num_edges));
    }

    virtual void add_edge(vid_t from, vid_t to)
    {
        if (to == from) {
            LOG(WARNING) << "Tried to add self-edge " << from << "->" << to
                         << std::endl;
            return;
        }

        num_edges++;
        from = get_vid(from);
        to = get_vid(to);
        degrees[from]++;
        degrees[to]++;

        fout.write((char *)&from, sizeof(vid_t));
        fout.write((char *)&to, sizeof(vid_t));
    }

    virtual void finalize() {
        fout.seekp(0);
        fout.write((char *)&num_vertices, sizeof(num_vertices));
        fout.write((char *)&num_edges, sizeof(num_edges));
        fout.close();

        fout.open(degree_name(basefilename), std::ios::binary);
        fout.write((char *)&degrees[0], num_vertices * sizeof(vid_t));
        fout.close();
    }
};

void convert(std::string basefilename, Converter *converter);

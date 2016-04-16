#pragma once

#include <utility>
#include <stdint.h>
#include <sys/stat.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "threadpool11/threadpool11.hpp"

#define rep(i, n) for (int i = 0; i < (int)(n); ++i)

typedef uint32_t vid_t;
typedef std::pair<vid_t, vid_t> edge_t;

extern threadpool11::Pool pool;

void writea(int f, char *buf, size_t nbytes);

inline std::string binedgelist_name(const std::string &basefilename)
{
    std::stringstream ss;
    ss << basefilename << ".binedgelist";
    return ss.str();
}

inline std::string degree_name(const std::string &basefilename)
{
    std::stringstream ss;
    ss << basefilename << ".degree";
    return ss.str();
}

inline bool is_exists(const std::string &name)
{
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
}

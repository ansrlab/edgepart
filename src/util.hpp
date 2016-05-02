#pragma once

#include <utility>
#include <chrono>
#include <stdint.h>
#include <sys/stat.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "threadpool11/threadpool11.hpp"

#define rep(i, n) for (int i = 0; i < (int)(n); ++i)
#define repv(i, n) for (vid_t i = 0; i < n; ++i)

DECLARE_int32(p);
DECLARE_uint64(memsize);
DECLARE_string(filename);
DECLARE_string(filetype);

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

inline std::string partitioned_name(const std::string &basefilename)
{
    std::stringstream ss;
    ss << basefilename << ".edgepart";
    return ss.str();
}

inline bool is_exists(const std::string &name)
{
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
}

class Timer
{
  private:
    std::chrono::system_clock::time_point t1, t2;
    double total;

  public:
    Timer() : total(0) {}
    void start() { t1 = std::chrono::system_clock::now(); }
    void stop()
    {
        t2 = std::chrono::system_clock::now();
        std::chrono::duration<double> diff = t2 - t1;
        total += diff.count();
    }
    double get_time() { return total; }
};

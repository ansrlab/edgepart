#pragma once

#include <string>
#include <sstream>
#include <iostream>
#include <fstream>

#include "util.hpp"

template <typename vertex_type, typename proc_type>
struct edgepart_writer {
    char *buffer;
    std::ofstream fout;

    edgepart_writer(const std::string &basefilename)
        : fout(partitioned_name(basefilename))
    {
        size_t s = std::max(sizeof(vertex_type) + sizeof(proc_type),
                            sizeof(vertex_type) + sizeof(vertex_type) +
                                sizeof(proc_type));
        buffer = new char[sizeof(char) + s];
    }

    ~edgepart_writer() { delete[] buffer; }

    /**
     * Replaces \255 with \255\1
     * Replaces \\n with \255\0
     */
    static std::string escape_newline(char *ptr, size_t strmlen)
    {
        size_t ctr = 0;
        for (size_t i = 0; i < strmlen; ++i) {
            ctr += (ptr[i] == (char)255 || ptr[i] == '\n');
        }

        std::string ret(ctr + strmlen, 0);

        size_t target = 0;
        for (size_t i = 0; i < strmlen; ++i, ++ptr) {
            if ((*ptr) == (char)255) {
                ret[target] = (char)255;
                ret[target + 1] = 1;
                target += 2;
            } else if ((*ptr) == '\n') {
                ret[target] = (char)255;
                ret[target + 1] = 0;
                target += 2;
            } else {
                ret[target] = (*ptr);
                ++target;
            }
        }
        CHECK_EQ(target, ctr + strmlen);
        return ret;
    }

    void save_vertex(vertex_type v, proc_type proc)
    {
        buffer[0] = 0;
        memcpy(buffer + 1, &v, sizeof(vertex_type));
        memcpy(buffer + 1 + sizeof(vertex_type), &proc,
               sizeof(proc_type));
        std::string result = escape_newline(
            buffer, 1 + sizeof(vertex_type) + sizeof(proc_type));
        fout << result << '\n';
    }

    void save_edge(vertex_type from, vertex_type to, proc_type proc)
    {
        buffer[0] = 1;
        memcpy(buffer + 1, &from, sizeof(vertex_type));
        memcpy(buffer + 1 + sizeof(vertex_type), &to, sizeof(vertex_type));
        memcpy(buffer + 1 + sizeof(vertex_type) + sizeof(vertex_type), &proc,
               sizeof(proc_type));
        std::string result = escape_newline(buffer, 1 + sizeof(vertex_type) +
                                                        sizeof(vertex_type) +
                                                        sizeof(proc_type));
        fout << result << '\n';
    }
};

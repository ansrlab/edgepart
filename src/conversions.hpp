#pragma once

#include "util.hpp"

#include <string>

DECLARE_string(filetype);

class Converter
{
  public:
    virtual bool done() = 0;
    virtual void init() = 0;
    virtual void add_edge(vid_t from, vid_t to) = 0;
    virtual void finalize() = 0;
};

void convert(std::string basefilename, Converter *converter);

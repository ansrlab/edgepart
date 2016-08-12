#pragma once

#include "util.hpp"

class Partitioner
{
  protected:
    Timer total_time;

  public:
    virtual void split() = 0;
};

#include <iterator>
#include <algorithm>
#include <functional>

#include "util.hpp"

threadpool11::Pool pool;

void preada(int f, char *buf, size_t nbytes, size_t off)
{
    size_t nread = 0;
    while (nread < nbytes) {
        ssize_t a = pread(f, buf, nbytes - nread, off + nread);
        PCHECK(a != ssize_t(-1)) << "Could not read " << (nbytes - nread)
                                 << " bytes!";
        buf += a;
        nread += a;
    }
}

void reada(int f, char *buf, size_t nbytes)
{
    size_t nread = 0;
    while (nread < nbytes) {
        ssize_t a = read(f, buf, nbytes - nread);
        PCHECK(a != ssize_t(-1)) << "Could not read " << (nbytes - nread)
                                 << " bytes!";
        buf += a;
        nread += a;
    }
}

void writea(int f, char *buf, size_t nbytes)
{
    size_t nwritten = 0;
    while (nwritten < nbytes) {
        ssize_t a = write(f, buf, nbytes - nwritten);
        PCHECK(a != ssize_t(-1)) << "Could not write " << (nbytes - nwritten)
                                 << " bytes!";
        buf += a;
        nwritten += a;
    }
}

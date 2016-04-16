#include "util.hpp"

threadpool11::Pool pool;

void writea(int f, char *buf, size_t nbytes)
{
    size_t nwritten = 0;
    while (nwritten < nbytes) {
        size_t a = write(f, buf, nbytes - nwritten);
        if (a == size_t(-1)) {
            LOG(FATAL) << "Could not write " << (nbytes - nwritten) << " bytes!"
                       << " error: " << strerror(errno) << std::endl;
        }
        buf += a;
        nwritten += a;
    }
}

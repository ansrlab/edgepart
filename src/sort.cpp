#include <algorithm>
#include <fcntl.h>
#include <iostream>
#include <queue>
#include <stdio.h>
#include <unistd.h>

#include "sort.hpp"
#include "util.hpp"

using namespace std;

struct mergeItem {
    uint64_t value;
    uint64_t srcIndex;

    bool operator<(const mergeItem &a) const { return a.value < value; }
};

template <typename T> struct fileBuffer {
    FILE *srcFile;
    T *buffer;
    uint64_t length;
    uint64_t index;
};

void externalSort(int fdInput, uint64_t size, int fdOutput, uint64_t memSize)
{
    if (size == 0) {
        // nothing to do here
        return;
    }

    CHECK_GT(memSize, sizeof(uint64_t)) << "Not enough memory for sorting";

    /*** STEP 1: Chunking ***/

    // how many values fit into one chunk
    const uint64_t chunkSize = min(memSize / sizeof(uint64_t), size);

    // number of chunks we must split the input into
    const uint64_t numChunks = (size + chunkSize - 1) / chunkSize;

    DLOG(INFO) << "filesize: " << (size * sizeof(uint64_t))
               << " bytes, memSize: " << memSize
               << " bytes, chunkSize: " << chunkSize
               << ", numChunks: " << numChunks;

    vector<FILE *> chunkFiles;
    chunkFiles.reserve(numChunks);

    vector<uint64_t> memBuf;
    memBuf.resize(chunkSize);

    for (uint64_t i = 0; i < numChunks; i++) {
        // read one chunk of input into memory
        size_t valuesToRead = chunkSize;
        if (size - i * chunkSize < valuesToRead) {
            valuesToRead = size - i * chunkSize;
            memBuf.resize(valuesToRead);
        }
        size_t bytesToRead = valuesToRead * sizeof(uint64_t);

        DLOG(INFO) << "chunk #" << i << " bytesToRead: " << bytesToRead;

        reada(fdInput, (char *)&memBuf[0], bytesToRead);

        // sort the values in the chunk
        sort(memBuf.begin(), memBuf.end());

        // open a new temporary file and save the chunk externally
        FILE *tmpf = tmpfile();
        PCHECK(tmpf != NULL) << "Creating a temporary file failed!";
        chunkFiles.push_back(tmpf);

        fwrite(&memBuf[0], sizeof(uint64_t), valuesToRead, tmpf);
        PCHECK(!ferror(tmpf)) << "Writing chunk to file failed!";
    }

    /*** STEP 2: k-way merge chunks ***/

    // priority queue used for n-way merging values from n chunks
    priority_queue<mergeItem> queue;

    // We reuse the already allocated memory from the memBuf and split the
    // available memory between numChunks chunk buffers and 1 output buffer.
    const size_t bufSize = chunkSize / (numChunks + 1);

    // input buffers (from chunks)
    vector<fileBuffer<uint64_t>> inBufs;
    inBufs.reserve(numChunks);

    for (uint64_t i = 0; i < numChunks; i++) {
        fileBuffer<uint64_t> buf = {
            chunkFiles[i],        /* srcFile */
            &memBuf[i * bufSize], /* buffer  */
            bufSize,              /* length  */
            0                     /* index   */
        };

        rewind(buf.srcFile);
        PCHECK(fread(buf.buffer, sizeof(uint64_t), bufSize, buf.srcFile) ==
               bufSize)
            << "Reading values from tmp chunk file #" << i << " failed";
        queue.push(mergeItem{buf.buffer[0], i});
        buf.index++;
        buf.length--;

        inBufs.push_back(buf);
    }

    // output buffer
    uint64_t *outBuf = &memBuf[numChunks * bufSize];
    uint64_t outLength = 0;

    while (!queue.empty()) {
        // put min item in buffer
        mergeItem top = queue.top();
        queue.pop();
        outBuf[outLength] = top.value;
        outLength++;

        // flush buffer to file, if buffer is full
        if (outLength == bufSize - 1) {
            writea(fdOutput, (char *)outBuf, outLength * sizeof(uint64_t));
            outLength = 0;
        }

        // refill input buffer, from which the value was taken
        // if length == 0 then the chunk was completely read
        fileBuffer<uint64_t> &srcBuf = inBufs[top.srcIndex];
        if (srcBuf.length > 0) {
            queue.push(mergeItem{srcBuf.buffer[srcBuf.index], top.srcIndex});
            srcBuf.index++;
            srcBuf.length--;
            if (srcBuf.length == 0 && srcBuf.srcFile) {
                DLOG(INFO) << "refilling inBuf #" << top.srcIndex;

                srcBuf.index = 0;
                srcBuf.length = fread(srcBuf.buffer, sizeof(uint64_t), bufSize,
                                      srcBuf.srcFile);

                // Close the file stream when it is completely read
                if (srcBuf.length < bufSize) {
                    fclose(srcBuf.srcFile);
                    srcBuf.srcFile = NULL;
                    DLOG(INFO) << "merged all data from chunk #"
                               << top.srcIndex;
                }
            }
        }
    }

    // flush rest, if output buffer is not already empty
    if (outLength > 0)
        writea(fdOutput, (char *)outBuf, outLength * sizeof(uint64_t));
}

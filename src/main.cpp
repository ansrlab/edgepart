#include <string>
#include <chrono>

#include "util.hpp"
#include "conversions.hpp"
#include "neighbor_partitioner.hpp"

DECLARE_bool(help);
DECLARE_bool(helpshort);

DEFINE_int32(p, 10, "number of parititions");
DEFINE_uint64(memsize, 4096, "memory size in megabytes");
DEFINE_string(filename, "", "the file name of the input graph");
DEFINE_string(filetype, "edgelist",
              "the type of input file (supports 'edgelist' and 'adjlist')");

int main(int argc, char *argv[])
{
    std::string usage = "-filename <path to the input graph> "
                        "[-filetype <edgelist|adjlist>] "
                        "[-p <number of partitions>] "
                        "[-memsize <memory budget in MB>]";
    google::SetUsageMessage(usage);
    google::ParseCommandLineNonHelpFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1; // output log to stderr
    if (FLAGS_help) {
        FLAGS_help = false;
        FLAGS_helpshort = true;
    }
    google::HandleCommandLineHelpFlags();

    auto start = std::chrono::system_clock::now();
    convert(FLAGS_filename);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> diff = end - start;
    LOG(INFO) << "shuffle time: " << diff.count();

    start = end;
    NeighborPartitioner partitioner(FLAGS_filename);
    partitioner.split();
    end = std::chrono::system_clock::now();
    diff = end - start;
    LOG(INFO) << "partition time: " << diff.count();
}

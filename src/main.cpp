#include <string>

#include "util.hpp"
#include "conversions.hpp"
#include "neighbor_partitioner.hpp"
#include "random_partitioner.hpp"
#include "dbh_partitioner.hpp"

DECLARE_bool(help);
DECLARE_bool(helpshort);

DEFINE_int32(p, 10, "number of parititions");
DEFINE_uint64(memsize, 4096, "memory size in megabytes");
DEFINE_string(filename, "", "the file name of the input graph");
DEFINE_string(filetype, "edgelist",
              "the type of input file (supports 'edgelist' and 'adjlist')");
DEFINE_double(sample_ratio, 2, "the sample size divided by num_vertices");
DEFINE_string(method, "neighbor",
              "partition method: neighbor, random, and dbh");

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

    Timer timer;
    timer.start();

    Timer shuffle_timer;
    shuffle_timer.start();
    convert(FLAGS_filename);
    shuffle_timer.stop();
    LOG(INFO) << "shuffle time: " << shuffle_timer.get_time();

    Timer partition_timer;
    partition_timer.start();
    Partitioner *partitioner = NULL;
    if (FLAGS_method == "neighbor")
        partitioner = new NeighborPartitioner(FLAGS_filename);
    else if (FLAGS_method == "random")
        partitioner = new RandomPartitioner(FLAGS_filename);
    else if (FLAGS_method == "dbh")
        partitioner = new DbhPartitioner(FLAGS_filename);
    else
        LOG(ERROR) << "unkown method: " << FLAGS_method;
    LOG(INFO) << "partition method: " << FLAGS_method;
    partitioner->split();
    partition_timer.stop();
    LOG(INFO) << "partition time: " << partition_timer.get_time();

    timer.stop();
    LOG(INFO) << "total time: " << timer.get_time();
}

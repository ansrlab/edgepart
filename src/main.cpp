#include <string>

#include "util.hpp"
#include "conversions.hpp"
#include "neighbor_partitioner.hpp"

DECLARE_bool(help);
DECLARE_bool(helpshort);
DEFINE_string(filename, "", "the file name of the input graph");
DEFINE_string(filetype, "edgelist",
              "the type of input file (supports 'edgelist' and 'adjlist')");

int main(int argc, char *argv[])
{
    std::string usage = "this program shuffle the input graph randomly";
    google::SetUsageMessage(usage);
    google::ParseCommandLineNonHelpFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1; // output log to stderr
    if (FLAGS_help) {
        FLAGS_help = false;
        FLAGS_helpshort = true;
    }
    google::HandleCommandLineHelpFlags();

    convert(FLAGS_filename);

    NeighborPartitioner partitioner(FLAGS_filename);
    partitioner.split();
}

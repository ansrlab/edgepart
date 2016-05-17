#include <string>
#include <fstream>

#include "util.hpp"
#include "conversions.hpp"

DECLARE_bool(help);
DECLARE_bool(helpshort);

DEFINE_uint64(memsize, 4096, "memory size in megabytes");
DEFINE_string(filename, "", "the file name of the input graph");
DEFINE_string(filetype, "edgelist",
              "the type of input file (supports 'edgelist' and 'adjlist')");

class Graph2Edgelist : public Converter
{
  private:
    std::string basefilename;
    std::ofstream fout;
  public:
    Graph2Edgelist(std::string basefilename)
        : basefilename(basefilename)
    {
    }
    bool done() { return is_exists(basefilename + ".edgelist"); }
    void init() { fout.open(basefilename + ".edgelist"); }
    void add_edge(vid_t from, vid_t to) { fout << from << ' ' << to << '\n'; }
    void finalize() { fout.close(); }
};

int main(int argc, char *argv[])
{
    std::string usage = "-filename <path to the input graph> "
                        "[-filetype <edgelist|adjlist>]";
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

    convert(FLAGS_filename, new Graph2Edgelist(FLAGS_filename));
    timer.stop();
    LOG(INFO) << "total time: " << timer.get_time();
}

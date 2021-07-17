#include "distributed_reachability.h"
#include "testH/DAGgenerator.h"
#include "testH/level_deciding.h"
#include "testH/dfsop.h"
#include "testH/butterfly.h"
#include <mpi.h>

int main(int argc, char *argv[]) {
    init_workers(&argc,&argv,MPI_THREAD_SINGLE);
    char *inputpath = "/reachability/input/testVertex4";
    char *outputpath = "/reachability/output/testVertex4";

    vector<vector<size_t> > graph;
    vector<size_t> levels;
    if (get_worker_id() == MASTER_RANK) {
        //write graph
        graph = daggenerator::gen(20000, 4, 8, 3);
//		daggenerator::outputDAG(graph);
        levels = level_decide::decide_lvl(graph);
        dfsop::writeDFS(graph, levels, inputpath);

    }

    printf("Hello!\n");
    setting::env=setting::VM;

    set_ghost_threshold(2);
    build_indexes(inputpath, outputpath, true);

    if (get_worker_id() == MASTER_RANK) {
        //read result
        vector<vector<size_t> > in_label, ou_label, lin, lout;

        //in_label,ou_label
        dfsop::readDFS(in_label, ou_label, outputpath);
        //lin,lout
        butterfly::TOLIndexQuery(graph, levels, lin, lout);

        //output index
//		{
//			for (uint vid = 0; vid < in_label.size(); ++vid) {
//				sort(in_label[vid].begin(), in_label[vid].end());
//				sort(ou_label[vid].begin(), ou_label[vid].end());
//
//				printf("vid:%d ", vid);
//				printf("in[");
//				for (uint i = 0; i < in_label[vid].size(); ++i) {
//					printf("%d, ", in_label[vid][i]);
//				}
//				printf("] ");
//				printf("ou[");
//				for (uint i = 0; i < ou_label[vid].size(); ++i) {
//					printf("%d, ", ou_label[vid][i]);
//				}
//				printf("] ");
//				printf("\n");
//			}
//		}
        //validatioin
        butterfly::validation(graph, levels, lin, lout, in_label, ou_label);

    }

    worker_finalize();
    return 0;
}

#include <unistd.h>
#include "reachO3/Worker.h"
#include "reachO0/WorkerO0.h"
#include "reachO1/reachO1.h"
#include "reachO2a/WorkerO2a.h"
#include "reachO4/WorkerO4.h"
#include "reachO4a/WorkerO4a.h"
#include "reachO4b/WorkerO4b.h"
#include "reachO4m1/WorkerO4m1.h"
#include "reachO5/WorkerO5.h"
#include "reachO3new/Worker.h"
#include "reachO3a/Worker.h"
#include "delBFS_analysis/Worker_delana.h"
#include "agent_analysis/Worker_agentana.h"
#include "utils/CmdOptsParser.h"


int main(int argc, char *argv[]) {
	//=====================================================initialize environment=======================================
	
	init_workers(&argc, &argv, setting::mpi_thread_level);
	if (MASTER_RANK == me) {
		cout << "running command: ";
		for (int i = 0; i < argc; i++) {
			cout << argv[i] << " ";
		}
		cout << endl;
	}
	
	// parsing args
	char *arg;
	
	if ((arg = getCmdOption(argc, argv, "-mpiThreadLevel")) != NULL) {
		if (strcmp(arg, "MPI_THREAD_SINGLE") == 0) setting::mpi_thread_level = MPI_THREAD_SINGLE;
		if (strcmp(arg, "MPI_THREAD_FUNNELED") == 0) setting::mpi_thread_level = MPI_THREAD_FUNNELED;
		if (strcmp(arg, "MPI_THREAD_SERIALIZED") == 0) setting::mpi_thread_level = MPI_THREAD_SERIALIZED;
		if (strcmp(arg, "MPI_THREAD_MULTIPLE") == 0) setting::mpi_thread_level = MPI_THREAD_MULTIPLE;
	} else {//default
		setting::mpi_thread_level = MPI_THREAD_SINGLE;
	}
	
	if ((arg = getCmdOption(argc, argv, "-nThreads")) != NULL) {
		setting::threads_num = atoi(arg);
	} else {//default
		setting::threads_num = 1;
	}
	if ((arg = getCmdOption(argc, argv, "-maxBsize")) != NULL) {
		setting::max_Bsize = atoi(arg);
	} else {//default
		setting::max_Bsize = 262144;
	}
	if ((arg = getCmdOption(argc, argv, "-iniBsize")) != NULL) {
		setting::ini_Bsize = atoi(arg);
	} else {//default
		setting::ini_Bsize = 2;
	}
	if ((arg = getCmdOption(argc, argv, "-bsize_growrate")) != NULL) {
		setting::Batch_grow_factor = std::stod(arg);
	} else {
		setting::Batch_grow_factor = 2;
	}
	
	if ((arg = getCmdOption(argc, argv, "-if")) != NULL) {
		setting::inputFileDirectory = arg;
	} else {//default
		if (MASTER_RANK == me) {
			cerr << "error: please provide input file path with -if flag" << endl;
		}
		exit(-1);
	}
	
	if ((arg = getCmdOption(argc, argv, "-of")) != NULL) {
		setting::outputFileDirectory = arg;
	} else {
		setting::outputFileDirectory = "";
		if (MASTER_RANK == me) {
			cout << "warn: no output file dir specified." << endl;
		}
	}
	
	if ((arg = getCmdOption(argc, argv, "-ghostThreshold")) != NULL) {
		setting::ghostThreshold = atoi(arg);
	} else {//default
		setting::ghostThreshold = 1;
	}
	
	if ((arg = getCmdOption(argc, argv, "-O")) != NULL) {
		setting::optimization_level = arg;
	} else {
		setting::optimization_level = "3";
	}
	
	
	if ((arg = getCmdOption(argc, argv, "-timelimit")) != NULL) {
		setting::runtimelimit = std::stod(arg);
	} else {
		setting::runtimelimit = 3600 * 2;
	}




//    log_general("optimization level is: %d", setting::optimization_level)
	//setHDFSHost(setting::HDFSHost);
	//====================================================running=======================================================
	if (setting::optimization_level == "0") {
//        log_general("building indexes with O0 optimization.")
		O0::build_indexes();
	} else if (setting::optimization_level == "1") {
		O1::build_indexO1();
	} else if (setting::optimization_level == "2a") {
		O2a::build_indexes();
	} else if (setting::optimization_level == "3") {
		O3::build_indexes();
	} else if (setting::optimization_level == "4m1") {
		O4m1::build_indexes();
	} else if (setting::optimization_level == "4") {
		O4::build_indexes();
	} else if (setting::optimization_level == "4a") {
		O4a::build_indexes();
	} else if (setting::optimization_level == "4b") {
		O4b::build_indexes();
	} else if (setting::optimization_level == "3new") {
		O3new::build_indexes();
	} else if (setting::optimization_level == "3a") {
		O3a::build_indexes();
	} else if (setting::optimization_level == "delana") {
		delana::build_indexes();
	} else if (setting::optimization_level == "agentana") {
		agentana::build_indexes();
	} else if (setting::optimization_level == "5") {
		O5::build_indexes();
	}
	worker_finalize();
	return 0;
}

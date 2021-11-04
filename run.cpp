#include <unistd.h>
#include "WorkerO4.h"
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
      cout << "warning: no output file dir specified." << endl;
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
    setting::optimization_level = "4";
  }

  if ((arg = getCmdOption(argc, argv, "-timelimit")) != NULL) {
    setting::runtimelimit = std::stod(arg);
  } else {
    setting::runtimelimit = 3600 * 2;
  }

  O4::build_indexes();

  worker_finalize();
  return 0;
}

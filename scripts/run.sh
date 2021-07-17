#!/bin/bash
#mpiexec -f conf -n 2 ./a.out -mpiThreadLevel MPI_THREAD_SINGLE -if input -of output -ghostThreshold 2 -HDFSHost localhost
#mpiexec -f conf -n 2 ./a.out -mpiThreadLevel MPI_THREAD_SINGLE -if input -of /scratch/zjh -ghostThreshold 2 -HDFSHost localhost

#mpiexec -f conf -n 12 ./a.out \
##piThreadLevel MPI_THREAD_SINGLE \
#-if /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.VBDuL.parts \
#-of /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.dist.indexes \
#-ghostThreshold 2 -HDFSHost localhost

#profile the program
mpiexec -f conf -n 1 valgrind --tool=callgrind ./a.out \
  -mpiThreadLevel MPI_THREAD_SINGLE \
  -if /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.VBDuL.parts \
  -of /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.dist.indexes \
  -ghostThreshold 2 -HDFSHost localhost

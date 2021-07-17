#set -x
#make
#mv ../a.out ./a.out
#mpiexec -n 1 ../a.out \
#  -mpiThreadLevel MPI_THREAD_SINGLE \
#  -if /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.VBDuL.parts \
#  -of /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.dist.indexes \
#  -ghostThreshold 2 -HDFSHost localhost -O 0

#if [ $? == "0" ]; then
#fi

#mpiexec -n 1 valgrind --leak-check=full --track-origins=yes -v ./a.out \
#  -mpiThreadLevel MPI_THREAD_SINGLE \
#  -if /home/zjh/reach/TestData/test_graph.VBDuL.parts \
#  -of /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.dist.indexes \
#  -ghostThreshold 2 -HDFSHost localhost -O 0

#mpiexec -n 1 ./a.out \
#  -mpiThreadLevel MPI_THREAD_SINGLE \
#  -if /home/zjh/reach/TestData/test_graph.VBDuL.parts \
#  -of /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.dist.indexes \
#  -ghostThreshold 2 -HDFSHost localhost -O 0

#mpiexec -n 1 valgrind --leak-check=full --track-origins=yes -v ./a.out \
#  -mpiThreadLevel MPI_THREAD_SINGLE \
#  -if /home/zjh/reach/TestData/test_graph.VBDuL.parts \
#  -of /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.dist.indexes \
#  -ghostThreshold 2 -HDFSHost localhost -O 1

#mpic++ run.cpp -O2 -std=c++11 -g -Wall

#mpiexec -n 1 valgrind --leak-check=full --track-origins=yes -v ./a.out \
#  -mpiThreadLevel MPI_THREAD_SINGLE \
#  -if /home/junhzhan/reach/TestData/test_graph.VBDuL.parts \
#  -of /tmp/result \
#  -ghostThreshold 2 -HDFSHost localhost -O 3

#mpiexec -n 4 ./a.out \
#  -mpiThreadLevel MPI_THREAD_SINGLE \
#  -if /home/junhzhan/reach/TestData/test_graph.VBDuL.parts \
#  -of /tmp/result \
#  -ghostThreshold 2 -HDFSHost localhost -O 3

#echo "abc"
#mpiexec -n 2 xterm -e gdb ../a.out \
#  -mpiThreadLevel MPI_THREAD_SINGLE \
#  -if /home/zjh/reach/TestData/test_graph.VBDuL.parts \
#  -of /data/junhzhan/Datasets/web_uk/uk-2007-05.scc.dist.indexes \
#  -ghostThreshold 2 -HDFSHost localhost -O 0

#mpiexec dist_reach -if /home/junhzhan/reach/TestData/test_graph.VBDuL.parts -O 2a

mpiexec dist_reach -if /scratch/rch/icde2019/citeseer/citeseer.VBDuL.parts -O 2a

#mpiexec -n 24 ./a.out \
#  -mpiThreadLevel MPI_THREAD_SINGLE \
#  -if /scratch/rch/icde2019/citeseerx/citeseerx.VBDuL.parts \
#  -of /tmp/result \
#  -ghostThreshold 2 -HDFSHost localhost -O 2a

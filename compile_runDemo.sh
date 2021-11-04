#!/usr/bin/bash

#set your gcc path
export PATH=/opt/rh/devtoolset-9/root/usr/bin:$PATH

#set env variables, and add corresponding env variables to .bashrc in each node
export PATH=$PATH:/usr/lib64/mpich/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib64/mpich/lib

#compile code
make clean
make

#create folder to store index
mkdir example/index
#create index, args:
# -if: directory to the input graph
# -of: directory for storing index
mpiexec -n 2 ./drl -if example/graph -of example/index

#process queries, args:
# arg0: directory to the index files
# arg1: path to the queries
./query example/index example/queries.txt
project(dist_reach)


#set(CMAKE_CXX_COMPILER "/home/zjh/mpi/inst/bin/mpicxx")
#set(CMAKE_CXX_COMPILER "/home/zjh/mpi/bin/mpicxx")
#set(CMAKE_CXX_COMPILER "/usr/lib64/mpich/bin/mpicxx")

#set(CMAKE_CXX_COMPILER "/home/junhzhan/bin/mpic++")

set(CMAKE_CXX_COMPILER "/usr/bin/mpic++")

#include_directories(/home/zjh/hadoop/hadoop-1.2.1/dist_reach/c++/libhdfs
#        /home/zjh/java/jdk/include
#        /home/zjh/java/jdk/include/linux)

#link_directories(/home/zjh/hadoop/hadoop-1.2.1/c++/Linux-amd64-64/lib)
#link_libraries(/home/zjh/hadoop/hadoop-1.2.1/c++/Linux-amd64-64/lib/libhdfs.a
#        /home/zjh/java/jdk/jre/lib/amd64/server/libjvm.so)

add_executable(dist_reach run.cpp
        utils/Aggregator.h utils/Combiner.h
        utils/communication.h
        utils/global.h utils/log.h utils/serialization.h utils/TeraSort.h utils/time.h utils/type.h
        utils/vecs.h utils/ydhdfs.h utils/ydhdfs1.h utils/ydhdfs2.h utils/CmdOptsParser.h
        utils/GraphIO.h utils/hash_set.h
        WorkerO4.h utils/statistics.h)
add_executable(test test.cpp)
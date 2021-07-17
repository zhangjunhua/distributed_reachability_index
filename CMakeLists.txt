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

add_executable(dist_reach run.cpp testrun.h
        reachO3/Vertex.h reachO3/Worker.h testH/butterfly.h testH/DAGgenerator.h
        testH/dfsop.h testH/header.h testH/level_deciding.h utils/Aggregator.h utils/Combiner.h
        utils/communication.h
        utils/global.h utils/log.h utils/serialization.h utils/TeraSort.h utils/time.h utils/type.h
        utils/vecs.h utils/ydhdfs.h utils/ydhdfs1.h utils/ydhdfs2.h utils/CmdOptsParser.h
        utils/GraphIO.h utils/hash_set.h
        reachO0/VertexO0.h reachO0/WorkerO0.h
        reachO1/reachO1.h reachO1/vertexO1.h reachO1/workerO1.h
        reachO2a/VertexO2a.h reachO2a/WorkerO2a.h
        reachO4/WorkerO4.h reachO4a/WorkerO4a.h reachO4b/WorkerO4b.h reachO4m1/WorkerO4m1.h reachO3new/Worker.h
        reacho5/WorkerO5.h
        delBFS_analysis/Vertex_delana.h delBFS_analysis/Worker_delana.h
        delBFS_analysis/statistics_delana.h delBFS_analysis/statistics_delana.h agent_analysis/Worker_agentana.h utils/statistics.h)

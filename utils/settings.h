//
// Created by giraph on 4/12/19.
//

#ifndef REACHABILITY_SETTINGS_H
#define REACHABILITY_SETTINGS_H

//#include <sys/types.h>

namespace setting {
    static u_int32_t threads_num = 1;
    static std::string inputFileDirectory;
    static std::string outputFileDirectory;
    static std::string HDFSHost;
    static uint32_t ghostThreshold;
    static uint32_t max_Bsize;
    static uint32_t ini_Bsize;
    static double Batch_grow_factor;
    static int mpi_thread_level;
    static std::string optimization_level;
    static double runtimelimit;
    //runtime enviroment
};


#endif //REACHABILITY_SETTINGS_H

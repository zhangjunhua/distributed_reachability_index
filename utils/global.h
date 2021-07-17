#ifndef GLOBAL_H
#define GLOBAL_H

#include <mpi.h>
#include <stddef.h>
#include <limits.h>
#include <string>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#define hash_map std::unordered_map
#define hash_set std::unordered_set

#include <assert.h> //for ease of debug

using namespace std;

string getfilename(string path) {
    size_t pos = path.rfind('/');
    if (pos == string::npos) {
        return path;
    } else {
        return path.substr(pos + 1, path.size() - pos);
    }
}

string now() {
    time_t t = time(0);
    char tmp[64];
    strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", localtime(&t));
    return string(tmp);
}

//#define __FILENAME__ (strrchr(__FILE__, '/')+1) // 文件名
#define log_general(format, ...){fprintf(stdout, "[%s %s.%s()#%u]" format "\n",now().c_str(),getfilename(__FILE__).c_str(), __func__,  __LINE__, ##__VA_ARGS__ );}
#define log_nodeid_superstep(format, ...){fprintf(stdout, "[%s %s.%s()#%u] rank#%d supstep#%d " format "\n",now().c_str(),getfilename(__FILE__).c_str(), __func__,  __LINE__, _my_rank,global_step_num, ##__VA_ARGS__ );}


#define record(format, ...){printf("[record of rank:%d dataset:%s iniBsize:%u opt_level:%s tcore:%d] " format "\n",\
me,setting::inputFileDirectory.c_str(),setting::ini_Bsize,setting::optimization_level.c_str(),np, ##__VA_ARGS__);}

//============================
///worker info
#define MASTER_RANK 0

namespace global {
    thread_local uint32_t thread_id;
}

int me;
int np;


void init_workers(int *argc, char ***argv, int required) {
    int provided;
    MPI_Init_thread(argc, argv, required, &provided);
    assert(provided == required);
    MPI_Comm_size(MPI_COMM_WORLD, &np);
    MPI_Comm_rank(MPI_COMM_WORLD, &me);
}

void worker_finalize() {
    MPI_Finalize();
}

void worker_barrier() {
    MPI_Barrier(MPI_COMM_WORLD);
}

//------------------------
// worker parameters

struct WorkerParams {
    string input_path;
    string output_path;
    bool force_write;
    bool native_dispatcher; //true if input is the output of a previous blogel job

    WorkerParams() {
        force_write = true;
        native_dispatcher = false;
    }
};

struct MultiInputParams {
    vector<string> input_paths;
    string output_path;
    bool force_write;
    bool native_dispatcher; //true if input is the output of a previous blogel job

    MultiInputParams() {
        force_write = true;
        native_dispatcher = false;
    }

    void add_input_path(string path) {
        input_paths.push_back(path);
    }
};

//============================
//general types
typedef uint32_t NODETYPE;

//============================
//global variables
int global_step_num;

inline int step_num() {
    return global_step_num;
}

int global_phase_num;

inline int phase_num() {
    return global_phase_num;
}

void *global_message_buffer = NULL;

inline void set_message_buffer(void *mb) {
    global_message_buffer = mb;
}

inline void *get_message_buffer() {
    return global_message_buffer;
}

int global_vnum = 0;


enum BITS {
    HAS_MSG_ORBIT = 0,
    FORCE_TERMINATE_ORBIT = 1,
    WAKE_ALL_ORBIT = 2
};
//currently, only 3 bits are used, others can be defined by users
char global_bor_bitmap;

void clearBits() {
    global_bor_bitmap = 0;
}

void setBit(int bit) {
    global_bor_bitmap |= (2 << bit);
}

int getBit(int bit, char bitmap) {
    return ((bitmap & (2 << bit)) == 0) ? 0 : 1;
}

void hasMsg() {
    setBit(HAS_MSG_ORBIT);
}

void wakeAll() {
    setBit(WAKE_ALL_ORBIT);
}

void forceTerminate() {
    setBit(FORCE_TERMINATE_ORBIT);
}

//====================================================
#define ROUND 11 //for PageRank


//=====================reachability==================
int batch_begin_lvl = 0;
int batch_end_lvl;
bool share_before_batch = true;

inline NODETYPE inBatchLevel(NODETYPE level) {
    return level - batch_begin_lvl;
}

int vid2workerid(NODETYPE key) {
    if (key >= 0)
        return key % np;
    else
        return (-key) % np;
}


double runtime = 0;
#endif

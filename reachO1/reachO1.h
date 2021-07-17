//
// Created by zjh on 9/9/19.
//

#ifndef REACHABILITY_REACHO1_H
#define REACHABILITY_REACHO1_H

#include <iostream>
#include "workerO1.h"

using namespace std;

namespace O1 {
    void build_indexO1() {
        log_general("build index with optimization level 1")
        worker wrk;
        wrk.run();
    }
}
#endif //REACHABILITY_REACHO1_H

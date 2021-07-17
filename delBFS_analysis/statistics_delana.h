//
// Created by 24148 on 27/02/2020.
//

#ifndef REACHABILITY_STATISTICS_DELANA_H
#define REACHABILITY_STATISTICS_DELANA_H

#include "Vertex_delana.h"

#define EDGE_TYPE unsigned long

namespace delana {
    struct statistics {
        std::string dataset = "";
        NODE_TYPE N = 0;
        EDGE_TYPE M = 0;

        EDGE_TYPE Vedgemsgcnt = 0;
        EDGE_TYPE Nedgemsgcnt = 0;

        EDGE_TYPE Vvrtxmsgcnt = 0;
        EDGE_TYPE Nvrtxmsgcnt = 0;


        double runtime = 0, start_time = 0, end_time = 0;
        EDGE_TYPE olbcount = 0;
        EDGE_TYPE ilbcount = 0;

        EDGE_TYPE Nbfscount = 0;
        EDGE_TYPE Vbfscount = 0;
        EDGE_TYPE N2Vcount = 0;

        EDGE_TYPE postbatch_Nbfscount = 0;
        EDGE_TYPE postbatch_Vbfscount = 0;

        //avg size of visit set at poost batch
        EDGE_TYPE postbatch_vertexcount = 0;


        //msg sent to
        EDGE_TYPE vmsg_count = 0;
        EDGE_TYPE nmsg_count = 0;

        EDGE_TYPE vertex_with_msg_count = 0;

        //block count
        EDGE_TYPE vmsg_query_block_count = 0;
        EDGE_TYPE vmsg_visit_block_count = 0;
        EDGE_TYPE nmsg_query_block_count = 0;
        EDGE_TYPE nmsg_vvisit_block_count = 0;
        EDGE_TYPE nmsg_nvisit_block_count = 0;
        EDGE_TYPE nmsg_identical_level_block_count = 0;

        EDGE_TYPE dbfs_turnpoint_count = 0;

        EDGE_TYPE vmsg_not_blocked_count = 0;
        EDGE_TYPE nmsg_not_blocked_count = 0;

/* need to add statistics for drl++ algorithm,
 * such as size of visit of each vertex in each batch.
*/

        void output_result() {
            std::ostringstream oss;
//            oss << dataset << " (N:" << N << ",M:" << M << ")";
            /**
             *         EDGE_TYPE Vedgemsgcnt = 0;
        EDGE_TYPE Nedgemsgcnt = 0;

        EDGE_TYPE Vvrtxmsgcnt = 0;
        EDGE_TYPE Nvrtxmsgcnt = 0;
             */
            oss << " statistics-> ";
            oss << " Vedgemsgcnt:" << Vedgemsgcnt;
            oss << " Nedgemsgcnt:" << Nedgemsgcnt;
            oss << " Vvrtxmsgcnt:" << Vvrtxmsgcnt;
            oss << " Nvrtxmsgcnt:" << Nvrtxmsgcnt;

//            oss << " runtime: " << runtime;
//            oss << " lbsize(i,o): (" << ilbcount << "," << olbcount << ")";
//            oss << " bfscnt(N,V,NtoV): (" << Nbfscount << "," << Vbfscount << "," << N2Vcount << ")";
//            oss << "postbatch_count(N,V): (" << postbatch_Nbfscount << "," << postbatch_Vbfscount << ")";
//            oss << " postbatch_vertexcount " << postbatch_vertexcount;
//
//            oss << " vmsg_count " << vmsg_count;
//            oss << " vmsg_query_block_count " << vmsg_query_block_count;
//            oss << " vmsg_visit_block_count " << vmsg_visit_block_count;
//            oss << " vmsg_not_blocked_count " << vmsg_not_blocked_count;
//
//            oss << " nmsg_count " << nmsg_count;
//            oss << " nmsg_query_block_count " << nmsg_query_block_count;
//            oss << " nmsg_vvisit_block_count " << nmsg_vvisit_block_count;
//            oss << " nmsg_nvisit_block_count " << nmsg_nvisit_block_count;
//            oss << " nmsg_identical_level_block_count " << nmsg_identical_level_block_count;
//            oss << " nmsg_not_blocked_count " << nmsg_not_blocked_count;
//            oss << " dbfs_turnpoint_count " << dbfs_turnpoint_count;
            std::cout << oss.str() << std::endl;
//        std::cout << nmsg_count - nmsg_query_block_count - nmsg_vvisit_block_count -
//                nmsg_nvisit_block_count - nmsg_not_blocked_count - nmsg_identical_level_block_count << std::endl;
            assert((nmsg_count - nmsg_query_block_count - nmsg_vvisit_block_count -
                    nmsg_nvisit_block_count - nmsg_not_blocked_count - nmsg_identical_level_block_count) == 0);
            assert((vmsg_count - vmsg_query_block_count - vmsg_visit_block_count - vmsg_not_blocked_count) == 0);


        }
    } sta;
}

#endif //REACHABILITY_STATISTICS_DELANA_H

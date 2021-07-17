#ifndef WORKERdelana_H
#define WORKERdelana_H

#include <vector>
#include <fstream>
#include "../utils/global.h"
#include <string>
#include "../utils/communication.h"
#include "../utils/Combiner.h"
#include "../utils/Aggregator.h"
#include "../utils/GraphIO.h"
#include <thread>
#include <sstream>
#include "../utils/settings.h"
#include "Vertex_delana.h"
#include "statistics_delana.h"

#define NONE                 "\e[0m"
#define L_GREEN              "\e[1;32m"
using namespace std;
namespace delana {


//===========================================Batch Relevant Code====================
    int batch = 0;
//an array define size of each batch4
    vector<int> batch_size;
    bool batch_first_supstep = true;
    double Bsize = 0;

    void for_hostlink() {
        batch_size.clear();
        uint32_t size = 1;
        while (size <= setting::max_Bsize) {
//	while (accu < 4096) { //262144, 8
            for (int i = 0; i < 200; i++) {
                batch_size.push_back(size);
            }
            size *= 2;
        }
    }

    void init_batch_size() {
//int batch_size[] = {1,2,4,8,16,32,64,128,256,512,1024,2048,4096,8192,16384,32768,65536,131072,262144};
//    const uint32_t max_Bsize = 8;
        uint32_t size = 1;
        while (size <= setting::max_Bsize) {
//	while (accu < 4096) { //262144, 8
            batch_size.push_back(size);
            size *= 2;
        }

//        for_hostlink();
    }

    struct mirror_vertex {
        mirror_vertex() {}

        mirror_vertex(NODETYPE id, vector<NODETYPE> &in, vector<NODETYPE> &ou) : id(id), in(in), out(ou) {}

        void set(NODETYPE id1, const vector<NODETYPE> &in1, const vector<NODETYPE> &out1) {
            id = id1;
            in = in1;
            out = out1;
        }

        NODETYPE id;
        vector<NODETYPE> in;
        vector<NODETYPE> out;
    };

    struct idx_communication {
        NODETYPE id;
        vector<NODETYPE> lb;
    };

    //store mirrors current batch index
    vector<mirror_vertex> mir;


    ibinstream &operator<<(ibinstream &m, const mirror_vertex &v) {
        m << v.id;
        m << v.in;
        m << v.out;
        return m;
    }

    obinstream &operator>>(obinstream &m, mirror_vertex &v) {
        m >> v.id;
        m >> v.in;
        m >> v.out;
        return m;
    }


    class Worker {
//    typedef vector<VertexT *> VertexContainer;
//    typedef typename VertexContainer::iterator VertexIter;
//
//    typedef typename VertexT::KeyType KeyT;
//    typedef typename VertexT::MessageType MessageT;
//    typedef typename VertexT::HashType HashT;
//
//    typedef MessageBuffer<VertexT> MessageBufT;
//    typedef typename MessageBufT::MessageContainerT MessageContainerT;
//    typedef typename MessageBufT::Map Map;
//    typedef typename MessageBufT::MapIter MapIter;


    public:
        Worker() : del_forward_gmsg(np), del_backward_gmsg(np), add_forward_gmsg(np), add_backward_gmsg(np),
                   oidx_msg(np), iidx_msg(np), oidx_bin(np), iidx_bin(np) {}

        void preprocess() {
            vector<vector<pair<NODETYPE, NODETYPE >>> iml(np), oml(np);
            for (unordered_map<NODETYPE, vector<NODETYPE >>::iterator it = in_ghosts.begin();
                 it != in_ghosts.end();
                 ++it) {
                NODETYPE maxlevel = 0;
                for (NODETYPE idx:it->second) {
                    maxlevel = max(maxlevel, vertexes[idx].level);
                }
                iml[vid2workerid(it->first)].emplace_back(it->first, maxlevel);
            }
            for (unordered_map<NODETYPE, vector<NODETYPE >>::iterator it = out_ghosts.begin();
                 it != out_ghosts.end();
                 ++it) {
                NODETYPE maxlevel = 0;
                for (NODETYPE idx:it->second) {
                    maxlevel = max(maxlevel, vertexes[idx].level);
                }
                oml[vid2workerid(it->first)].emplace_back(it->first, maxlevel);
            }
            Ibin_all_to_all_64(iml);
            Ibin_all_to_all_64(oml);
            for (char m = 0; m < iml.size(); ++m) {
                for (pair<NODETYPE, NODETYPE> &p:iml[m]) {
                    Vertex &v = vertexes[vid2idx(p.first)];
                    for (size_t i = 0; i < v.im.size(); ++i) {
                        if (v.im[i] == m)v.iml[i] = p.second;
                    }
                }
            }
            iml.clear(), iml.shrink_to_fit();

            for (char m = 0; m < oml.size(); ++m) {
                for (pair<NODETYPE, NODETYPE> &p:oml[m]) {
                    Vertex &v = vertexes[vid2idx(p.first)];
                    for (size_t i = 0; i < v.om.size(); ++i) {
                        if (v.om[i] == m)v.oml[i] = p.second;
                    }
                }
            }
            oml.clear(), oml.shrink_to_fit();
        }


        inline void broadcast(Vertex &v, NODETYPE level, bool add, bool forward) {
            if (add) {
                if (forward) {
                    for (size_t i = 0; i < v.om.size(); ++i) {
                        if (v.oml[i] >= batch_begin_lvl) {
                            add_forward_gmsg[v.om[i]].emplace_back(v.id, level);
                            if (!share_before_batch && v.om[i] != me && !oidx_bin[v.om[i]][level]) {
                                oidx_msg[v.om[i]].emplace_back(level);
                                oidx_bin[v.om[i]][level] = true;
                            }
                        }
                    }
                } else {
                    for (size_t i = 0; i < v.im.size(); ++i) {
                        if (v.iml[i] >= batch_begin_lvl) {
                            add_backward_gmsg[v.im[i]].emplace_back(v.id, level);
                            if (!share_before_batch && v.im[i] != me && !iidx_bin[v.im[i]][level]) {
                                iidx_msg[v.im[i]].emplace_back(level);
                                iidx_bin[v.im[i]][level] = true;
                            }
                        }
                    }
                }
            } else {
                if (forward) {
                    for (size_t i = 0; i < v.om.size(); ++i) {
                        if (v.oml[i] >= batch_begin_lvl) {
                            del_forward_gmsg[v.om[i]].emplace_back(v.id, level);
                            if (!share_before_batch && v.om[i] != me && !oidx_bin[v.om[i]][level]) {
                                oidx_msg[v.om[i]].emplace_back(level);
                                oidx_bin[v.om[i]][level] = true;
                            }
                        }
                    }
                } else {
                    for (size_t i = 0; i < v.im.size(); ++i) {
                        if (v.iml[i] >= batch_begin_lvl) {
                            del_backward_gmsg[v.im[i]].emplace_back(v.id, level);
                            if (!share_before_batch && v.im[i] != me && !iidx_bin[v.im[i]][level]) {
                                iidx_msg[v.im[i]].emplace_back(level);
                                iidx_bin[v.im[i]][level] = true;
                            }
                        }
                    }
                }
            }
        }

        void sync_label_msgs() {
            vector<ibinstream> ibss(np);
            //serialization
            for (int peer = 0; peer < np; ++peer) {
                if (peer != me) {
                    ibss[peer] << oidx_msg[peer].size();
                    for (size_t i = 0; i < oidx_msg[peer].size(); i++) {
                        ibss[peer] << oidx_msg[peer][i] << mir[oidx_msg[peer][i]].out;
                        oidx_bin[peer][oidx_msg[peer][i]] = false;
                    }
                    oidx_msg[peer].clear();
//                    oidx_msg[peer].shrink_to_fit();

                    ibss[peer] << iidx_msg[peer].size();
                    for (size_t i = 0; i < iidx_msg[peer].size(); i++) {
                        ibss[peer] << iidx_msg[peer][i] << mir[iidx_msg[peer][i]].in;
                        iidx_bin[peer][iidx_msg[peer][i]] = false;
                    }
                    iidx_msg[peer].clear();
//                    iidx_msg[peer].shrink_to_fit();
                }
            }
            //transfer
            vector<obinstream> obss = exchange_bin_data(ibss);
            size_t size;
            NODETYPE id;
            //deserialization
            for (int peer = 0; peer < np; ++peer) {
                if (peer != me) {
                    obss[peer] >> size;
                    for (size_t i = 0; i < size; ++i) {
                        obss[peer] >> id;
                        mir[id].id = id;
                        obss[peer] >> mir[id].out;
                    }
                    obss[peer] >> size;
                    for (size_t i = 0; i < size; ++i) {
                        obss[peer] >> id;
                        mir[id].id = id;
                        obss[peer] >> mir[id].in;
                    }
                }
            }
        }

        void batch_begin_compute() {
            map<NODETYPE, NODETYPE>::iterator it = lvl_v_idx.lower_bound(batch_begin_lvl);
            for (; it != lvl_v_idx.end() && it->first < batch_end_lvl; ++it) {
                Vertex &v = vertexes[it->second];
                broadcast(v, inBatchLevel(v.level), true, true);
                broadcast(v, inBatchLevel(v.level), true, false);
            }

            if (!share_before_batch) {
                sync_label_msgs();
            }
            Ibin_all_to_all_64(del_forward_gmsg);
            Ibin_all_to_all_64(del_backward_gmsg);
            Ibin_all_to_all_64(add_forward_gmsg);
            Ibin_all_to_all_64(add_backward_gmsg);
        }

        inline void status_update(NODETYPE idx) {
            if (!post_batch_bin[idx]) {
                post_batch_bin[idx] = true;
                post_batch_idx.emplace_back(idx);
            }
        }


        void batch_msg_compute() {
            vector<vector<pair<NODETYPE, NODETYPE >>>
                    _del_forward_gmsg(np), _del_backward_gmsg(np), _add_forward_gmsg(np), _add_backward_gmsg(np);
            swap(del_forward_gmsg, _del_forward_gmsg);
            swap(del_backward_gmsg, _del_backward_gmsg);
            swap(add_forward_gmsg, _add_forward_gmsg);
            swap(add_backward_gmsg, _add_backward_gmsg);
            for (vector<pair<NODETYPE, NODETYPE>> &a:_del_forward_gmsg) {
                for (pair<NODETYPE, NODETYPE> msg:a) {//id,level
                    sta.Vedgemsgcnt += out_ghosts[msg.first].size();
                    for (NODETYPE idx:out_ghosts[msg.first]) {
                        Vertex &v = vertexes[idx];
                        if (v.level < batch_begin_lvl)continue;
                        if (v.ist.find(msg.second) != v.ist.end() && !v.ist[msg.second])continue;//has beed deleted
                        if (set_intersection(v.olb, mir[msg.second].in))continue;
                        v.ist[msg.second] = false;
                        status_update(idx);
                        broadcast(v, msg.second, false, true);
                    }
                }
            }
            _del_forward_gmsg.clear();
            _del_forward_gmsg.shrink_to_fit();
            for (vector<pair<NODETYPE, NODETYPE>> &a:_del_backward_gmsg) {
                for (pair<NODETYPE, NODETYPE> msg:a) {
                    sta.Vedgemsgcnt += in_ghosts[msg.first].size();
                    for (NODETYPE idx:in_ghosts[msg.first]) {
                        Vertex &v = vertexes[idx];
                        if (v.level < batch_begin_lvl)continue;
                        if (v.ost.find(msg.second) != v.ost.end() && !v.ost[msg.second])continue;//has beed deleted
                        if (set_intersection(v.olb, mir[msg.second].in))continue;
                        v.ost[msg.second] = false;
                        status_update(idx);
                        broadcast(v, msg.second, false, false);
                    }
                }
            }
            _del_backward_gmsg.clear();
            _del_backward_gmsg.shrink_to_fit();
            for (vector<pair<NODETYPE, NODETYPE>> &a:_add_forward_gmsg) {
                for (pair<NODETYPE, NODETYPE> msg:a) {
                    sta.Nedgemsgcnt += out_ghosts[msg.first].size();
                    NODETYPE msglevel = msg.second + batch_begin_lvl;
                    for (NODETYPE idx:out_ghosts[msg.first]) {
                        Vertex &v = vertexes[idx];
                        if (v.level < batch_begin_lvl)continue;
                        if (v.ist.find(msg.second) != v.ist.end())continue;//has been added or deleted
                        if (set_intersection(v.ilb, mir[msg.second].out))continue;
                        if (msglevel < v.level) {
                            v.ist[msg.second] = true;
                            status_update(idx);
                            broadcast(v, msg.second, true, true);
                        } else if (msglevel > v.level) {
                            v.ist[msg.second] = false;
                            status_update(idx);
                            broadcast(v, msg.second, false, true);
                        }
                    }
                }
            }
            _add_forward_gmsg.clear();
            _add_forward_gmsg.shrink_to_fit();
            for (vector<pair<NODETYPE, NODETYPE>> &a:_add_backward_gmsg) {
                for (pair<NODETYPE, NODETYPE> msg:a) {
                    sta.Nedgemsgcnt += in_ghosts[msg.first].size();
                    NODETYPE msglevel = msg.second + batch_begin_lvl;
                    for (NODETYPE idx:in_ghosts[msg.first]) {
                        Vertex &v = vertexes[idx];
                        if (v.level < batch_begin_lvl)continue;
                        if (v.ost.find(msg.second) != v.ost.end())continue;//has been added or deleted
                        if (set_intersection(v.olb, mir[msg.second].in))continue;
                        if (msglevel < v.level) {
                            v.ost[msg.second] = true;
                            status_update(idx);
                            broadcast(v, msg.second, true, false);
                        } else if (msglevel > v.level) {
                            v.ost[msg.second] = false;
                            status_update(idx);
                            broadcast(v, msg.second, false, false);
                        }
                    }
                }
            }
            _add_backward_gmsg.clear();
            _add_backward_gmsg.shrink_to_fit();
            if (!share_before_batch)
                sync_label_msgs();
            Ibin_all_to_all_64(del_forward_gmsg);
            Ibin_all_to_all_64(del_backward_gmsg);
            Ibin_all_to_all_64(add_forward_gmsg);
            Ibin_all_to_all_64(add_backward_gmsg);
        }


        /**
         * prepare the indexes of vertexes in current batch
         */
        void prebatch() {
            mir.resize(batch_end_lvl - batch_begin_lvl);
            for (int i = 0; i < mir.size(); i++) {
                mir[i].id = -1;
                mir[i].in.clear();
                mir[i].in.shrink_to_fit();
                mir[i].out.clear();
                mir[i].out.shrink_to_fit();
            }


            if (share_before_batch) {
                vector<mirror_vertex> to_send;
                for (map<NODETYPE, NODETYPE>::iterator it = lvl_v_idx.lower_bound(batch_begin_lvl);
                     it != lvl_v_idx.end() && it->first < batch_end_lvl; ++it) {
                    Vertex &v = vertexes[it->second];
                    to_send.emplace_back(inBatchLevel(v.level), v.ilb, v.olb);
                }

                vector<mirror_vertex> to_get;
                all_to_all_Ibcast<mirror_vertex>(to_send, to_get);

                for (int i = 0; i < to_get.size(); i++) {
                    swap(mir[to_get[i].id], to_get[i]);
                }
                for (int i = 0; i < to_send.size(); i++) {
                    swap(mir[to_send[i].id], to_send[i]);
                }
            } else {
                for (map<NODETYPE, NODETYPE>::iterator it = lvl_v_idx.lower_bound(batch_begin_lvl);
                     it != lvl_v_idx.end() && it->first < batch_end_lvl; ++it) {
                    Vertex &v = vertexes[it->second];
                    mir[inBatchLevel(v.level)].set(inBatchLevel(v.level), v.ilb, v.olb);
                }
                for (int i = 0; i < np; i++) {
                    if (i != me) {
                        oidx_bin[i].resize(mir.size());
                        iidx_bin[i].resize(mir.size());
                    }
                }
            }
        }

        void postbatch() {
            for (NODETYPE idx:post_batch_idx) {
                Vertex &v = vertexes[idx];
                for (map<NODETYPE, bool>::iterator it = v.ost.begin(); it != v.ost.end(); ++it) {
                    if (it->second)v.olb.emplace_back(it->first + batch_begin_lvl);
                }
//                v.olb.shrink_to_fit();
                for (map<NODETYPE, bool>::iterator it = v.ist.begin(); it != v.ist.end(); ++it) {
                    if (it->second)v.ilb.emplace_back(it->first + batch_begin_lvl);
                }
//                v.ilb.shrink_to_fit();
                v.ist.clear();
                v.ost.clear();
                post_batch_bin[idx] = false;
            }
            post_batch_idx.clear();
//            post_batch_idx.shrink_to_fit();
        }

        vector<centralized_vertex> load_bin_graph() {
            //==============================================load graph==============================================
            vector<string> assignedSplits;
            //assign files to workers
            if (me == MASTER_RANK) {
                vector<vector<string>> arrangement =
                        dispatch_splits(getFileList(setting::inputFileDirectory), np);
                masterScatter(arrangement);
                assignedSplits = arrangement[0];
            } else {
                slaveScatter(assignedSplits);
            }

            vector<centralized_vertex> vtxs;

            for (string &path:assignedSplits) {

                FILE *file = fopen(path.c_str(), "rb");
                if (!file) {
                    log_general("file %s not found.", path.c_str())
                    exit(-1);
                }
                fseek(file, 0L, SEEK_END);
                vector<NODETYPE> buf(ftell(file) / 4);
                rewind(file);
                fread(buf.data(), sizeof(NODETYPE), buf.size(), file);
                fclose(file);
                size_t offset = 0;

                while (offset < buf.size()) {
                    centralized_vertex v;
                    v.id = buf[offset++];
                    v.level = buf[offset++];
                    v.ineb.resize(buf[offset++]);
                    for (size_t i = 0; i < v.ineb.size(); i++) {
                        v.ineb[i] = buf[offset++];
                    }
                    v.oneb.resize(buf[offset++]);
                    for (size_t i = 0; i < v.oneb.size(); i++) {
                        v.oneb[i] = buf[offset++];
                    }
                    vtxs.emplace_back(v);
                }
            }
            return vtxs;
        }

        void sync_graph(vector<centralized_vertex> graph) {
            //sync central vertex

            vector<vector<Vertex>> _loaded_parts(np);
            vector<vector<pair<NODETYPE, vector<NODETYPE >> >> _in_ghost(np), _ou_ghost(np);
            for (centralized_vertex &v:graph) {
                vector<Vertex> &vs = _loaded_parts[vid2workerid(v.id)];
                vs.emplace_back(Vertex(v));
                for (char M:vs.back().im) {
                    _in_ghost[M].emplace_back(make_pair(v.id, vector<NODETYPE>()));
                }
                for (char M:vs.back().om) {
                    _ou_ghost[M].emplace_back(make_pair(v.id, vector<NODETYPE>()));
                }
                for (NODETYPE neb:v.ineb) {
                    _in_ghost[vid2workerid(neb)].back().second.push_back(vid2idx(neb));
                }
                for (NODETYPE neb:v.oneb) {
                    _ou_ghost[vid2workerid(neb)].back().second.push_back(vid2idx(neb));
                }
            }
            graph.clear(), graph.shrink_to_fit();

            Iall_to_all_cat(_loaded_parts, _in_ghost, _ou_ghost);

            NODETYPE num_vertex = 0;
            for (vector<Vertex> &vs:_loaded_parts) {
                num_vertex += vs.size();
            }
            vertexes.resize(num_vertex);
            for (vector<Vertex> &vs:_loaded_parts) {
                for (Vertex &v:vs) {
                    v.iml.resize(v.im.size(), 0);
                    v.oml.resize(v.om.size(), 0);
                    swap(vertexes[vid2idx(v.id)], v);
                }
            }
            for (auto &a:_in_ghost) for (auto &b:a) swap(in_ghosts[b.first], b.second);
            for (auto &a:_ou_ghost) for (auto &b:a) swap(out_ghosts[b.first], b.second);
        }

        /*
         * many task to be done.
         * 1, read file
         * 2, sync graph
         * 3,
         */
        void construct_graph_from_file() {
            sync_graph(load_bin_graph());
            for (NODETYPE i = 0; i < vertexes.size(); i++) {
                lvl_v_idx[vertexes[i].level] = i;
            }
            preprocess();
        }


        // run the worker
        void run() {
            init_timers();
            ResetTimer(WORKER_TIMER);
            construct_graph_from_file();
            global_vnum = all_sum(vertexes.size());
            StopTimer(WORKER_TIMER);
            if (me == MASTER_RANK)
                cout << setting::inputFileDirectory << endl;
            PrintTimer("Load Time", WORKER_TIMER);
            worker_barrier();
            //=========================================================
            init_timers();
            ResetTimer(WORKER_TIMER);
            post_batch_bin.resize(vertexes.size(), false);
            runtime = get_current_time();


            //supersteps
            global_step_num = 0;
            uint64_t step_gmsg_num = 0, step_dgmsg_num = 0;
            uint64_t bat_gmsg_num = 0, bat_dgmsg_num = 0;

            uint64_t global_gmsg_num = 0, global_dgmsg_num = 0;
            //initializations
            init_batch_size();
            batch = 0;
            Bsize = setting::ini_Bsize;
            batch_begin_lvl = 0;
            ResetTimer(PRE_BATCH_TIMER);
            abc:
            while (true) {
                //prebatch goes here
                ResetTimer(BATCH_TIMER);
                batch_end_lvl = batch_begin_lvl + Bsize;
                StartTimer(PRE_BATCH_TIMER);
                prebatch();
                StopTimer(PRE_BATCH_TIMER);

                bat_gmsg_num = 0;
                bat_dgmsg_num = 0;
                batch_first_supstep = true;
                while (true) {
                    global_step_num++;
                    ResetTimer(SUPER_STEP_TIMER);
                    if (batch_first_supstep)
                        batch_begin_compute();
                    else
                        batch_msg_compute();

                    step_gmsg_num = 0;
                    for (NODETYPE i = 0; i < np; i++) {
                        step_gmsg_num += del_forward_gmsg[i].size();
                        step_gmsg_num += del_backward_gmsg[i].size();
                    }
                    step_dgmsg_num = step_gmsg_num;
                    for (NODETYPE i = 0; i < np; i++) {
                        step_gmsg_num += add_forward_gmsg[i].size();
                        step_gmsg_num += add_backward_gmsg[i].size();
                    }
                    step_gmsg_num = all_sum_LL(step_gmsg_num);
                    step_dgmsg_num = all_sum_LL(step_dgmsg_num);
                    bat_gmsg_num += step_gmsg_num;
                    bat_dgmsg_num += step_dgmsg_num;
                    global_gmsg_num += step_gmsg_num;
                    global_dgmsg_num += step_dgmsg_num;

//                    worker_barrier();
                    StopTimer(SUPER_STEP_TIMER);
//                    float pct = ((double) step_dgmsg_num) / ((double) step_gmsg_num) * 100;
//                    if (me == MASTER_RANK) {
//                        cout << "step " << global_step_num << " done. gmsgs#" << step_gmsg_num << " dgmsgs#"
//                             << step_dgmsg_num << "(" << pct << "%)" << " Time elapsed: "
//                             << get_timer(SUPER_STEP_TIMER)
//                             << " seconds"
//                             << endl;
//                    }
                    if (step_gmsg_num == 0)break;
                    batch_first_supstep = false;
                }
                //postbatch goes here
                postbatch();
                StopTimer(BATCH_TIMER);

                float pct = ((double) bat_dgmsg_num) / ((double) bat_gmsg_num) * 100;
                if (me == MASTER_RANK) {
                    printf(L_GREEN
                           "Batch %d of [%d,%d) with gmsg#%llu dmsg#%llu(%f\%) in sb(%d) done in %f seconds.\n" NONE,
                           batch, batch_begin_lvl, batch_end_lvl, bat_gmsg_num, bat_dgmsg_num, pct,
                           share_before_batch,
                           get_timer(BATCH_TIMER));
                }

                if (share_before_batch) {
//                    if (np * Bsize >= bat_gmsg_num * 2 && batch_end_lvl > (global_vnum / 4)) {
                    if (np * Bsize >= bat_gmsg_num * 2) {
                        share_before_batch = false;
                    }
                }

                batch_begin_lvl = batch_end_lvl;
                if (batch_begin_lvl >= global_vnum) {
                    break;
                }
                batch++;
//                if (batch == batch_size.size())
//                    batch--;
//                Bsize = batch_size[batch];
                Bsize = Bsize * setting::Batch_grow_factor;
                if (Bsize > setting::max_Bsize)
                    Bsize = setting::max_Bsize;
            }

            sta.Vvrtxmsgcnt = global_dgmsg_num;
            sta.Nvrtxmsgcnt = global_gmsg_num - global_dgmsg_num;

            StopTimer(WORKER_TIMER);
            runtime = get_current_time() - runtime;
            if (me == MASTER_RANK) record("runtime is %f", runtime)
            PrintTimer("Communication Time", COMMUNICATION_TIMER);
            PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
            PrintTimer("- Transfer Time", TRANSFER_TIMER);
            PrintTimer("Total Computational Time", WORKER_TIMER);
            PrintTimer("Total Pre_Batch Time", PRE_BATCH_TIMER);
            if (me == MASTER_RANK) {
                float pct = ((double) global_dgmsg_num) / ((double) global_gmsg_num) * 100;
                cout << "Total gmsgs#" << global_gmsg_num << " dgmsg#" << global_dgmsg_num << "(" << pct << "%)"
                     << endl;
            }
            worker_barrier();
            // dump graph
            ResetTimer(WORKER_TIMER);
//        dump_partition(params.output_path);
            {
                uint64_t idxsize = 0;
                for (Vertex &v:vertexes) {
                    idxsize += v.ilb.size();
                    idxsize += v.olb.size();
                }
                idxsize = master_sum_LL(idxsize);
                if (me == MASTER_RANK) {
                    record("index size is %lu.", idxsize)
                    sta.output_result();
                }
            }


            StopTimer(WORKER_TIMER);
            PrintTimer("Dump Time", WORKER_TIMER);
            worker_barrier();

//            output_status();
        }

        void output_status() {
            ostringstream oss;
            oss << "====================graph===================" << endl;
//            for (Vertex &v:vertexes) {
//                oss << v.id << " " << v.level << " in:{";
//                for (NODETYPE n:v.inebM) {
//                    oss << n << ",";
//                }
//                oss << "} out:{";
//                for (NODETYPE n:v.onebM) {
//                    oss << n << ",";
//                }
//                oss << "}" << endl;
//            }
//
//            oss << "====================ghost=================" << endl;
//            for (auto &e:in_ghost) {
//                oss << e.first << " ing{";
//                for (auto &e1:e.second) {
//                    oss << e1 << ",";
//                }
//                oss << "}" << endl;
//            }
//
//            for (auto &e:out_ghost) {
//                oss << e.first << " oug{";
//                for (auto &e1:e.second) {
//                    oss << e1 << ",";
//                }
//                oss << "}" << endl;
//            }

            oss << "====================index=================" << endl;
            for (Vertex &v:vertexes) {
                oss << v.id << " " << v.level << " ilb:{";
                for (NODETYPE n:v.ilb) {
                    oss << n << ",";
                }
                oss << "} olb:{";
                for (NODETYPE n:v.olb) {
                    oss << n << ",";
                }
                oss << "}" << endl;
            }

            cout << oss.str();
        }

    private:
        vector<Vertex> vertexes;
        hash_map<NODETYPE, vector<NODETYPE>> out_ghosts, in_ghosts; //vid->nbs's idx
        vector<bool> post_batch_bin;
        vector<NODETYPE> post_batch_idx;

        vector<vector<pair<NODETYPE, NODETYPE >>>
                del_forward_gmsg, del_backward_gmsg, add_forward_gmsg,
                add_backward_gmsg;
        vector<vector<NODETYPE>> oidx_msg, iidx_msg;
        vector<vector<bool>> oidx_bin, iidx_bin;

        map<NODETYPE, NODETYPE>
                lvl_v_idx;//level->pos in the vertexes
    };


    void build_indexes() {
        Worker worker;
        worker.run();
    }

}
#endif //WORKERdelana_H

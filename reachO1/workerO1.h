//
// Created by zjh on 9/9/19.
//

#ifndef REACHABILITY_WORKERO1_H
#define REACHABILITY_WORKERO1_H

#include <sstream>
#include "vertexO1.h"
#include "../utils/communication.h"
#include "../utils/settings.h"
#include "../utils/GraphIO.h"

namespace O1 {
    class worker {


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
            vector<vector<vertex>> _loaded_parts(np);
            vector<vector<pair<NODETYPE, vector<NODETYPE>>>> _in_ghost(np), _ou_ghost(np);
            for (centralized_vertex &v:graph) {
                vector<vertex> &vs = _loaded_parts[vid2workerid(v.id)];
                vs.emplace_back(vertex(v));
                for (char M:vs.back().inebM) {
                    _in_ghost[M].emplace_back(make_pair(v.id, vector<NODETYPE>()));
                }
                for (char M:vs.back().onebM) {
                    _ou_ghost[M].emplace_back(make_pair(v.id, vector<NODETYPE>()));
                }
                for (NODETYPE neb:v.ineb) {
                    _in_ghost[vid2idx[neb]].back().second.push_back(neb);
                }
                for (NODETYPE neb:v.oneb) {
                    _ou_ghost[vid2idx[neb]].back().second.push_back(neb);
                }
            }

            Iall_to_all_cat(_loaded_parts, _in_ghost, _ou_ghost);

            NODETYPE num_vertex = 0;
            for (vector<vertex> &vs:_loaded_parts) {
                num_vertex += vs.size();
            }
            vertexes.resize(num_vertex);
            NODETYPE index = 0;
            for (vector<vertex> &vs:_loaded_parts) {
                for (vertex &v:vs) {
                    swap(vertexes[index++], v);
                }
            }
            for (auto &a:_in_ghost) for (auto &b:a) swap(in_ghost[b.first], b.second);
            for (auto &a:_ou_ghost) for (auto &b:a) swap(out_ghost[b.first], b.second);
        }

        inline void broadcast_msg(vertex &v, message &msg, vector<ibinstream> &ibss) {
            if (!msg.is_backward()) {
                for (NODETYPE m:v.onebM) {
                    ibss[m] << v.id << msg;
                }
            } else {
                for (NODETYPE m:v.inebM) {
                    ibss[m] << v.id << msg;
                }
            }
        }


        inline void vertex_msg_compute(vertex &v, vector<message> &msgs, vector<ibinstream> &ibss) {
            for (message &m:msgs) {
                if (m.is_del()) {
                    if (m.is_forward()) {
                        if (v.fdv[m.level])continue;
                        else {
                            v.fdv[m.level] = true;
                            broadcast_msg(v, m, ibss);
                        }
                    } else {
                        if (v.bdv[m.level]) continue;
                        else {
                            v.bdv[m.level] = true;
                            broadcast_msg(v, m, ibss);
                        }
                    }
                }
            }

            for (message &m:msgs) {
                if (m.is_add()) {
                    if (m.level == v.level)continue;
                    if (m.is_forward()) {
                        if (v.fdv[m.level] || v.fav[m.level])continue;
                        else {
                            if (m.level < v.level) {
                                v.fav[m.level] = true;
                                v.ilb_cand.push_back(m.level);
                            } else {//m.level > v.level
                                v.fdv[m.level] = true;
                                m.set_del();
                            }
                            broadcast_msg(v, m, ibss);
                        }
                    } else {
                        if (v.bdv[m.level] || v.bav[m.level])continue;
                        else {
                            if (m.level < v.level) {
                                v.bav[m.level] = true;
                                v.olb_cand.push_back(m.level);
                            } else {//m.level > v.level
                                v.bdv[m.level] = true;
                                m.set_del();
                            }
                            broadcast_msg(v, m, ibss);
                        }
                    }
                }
            }
        }

        inline uint64_t unpack_msg_buf(vector<obinstream> &obss) {
            uint64_t msg_num = 0;
            NODETYPE vid;
            message msg;
            for (int i = 0; i < np; i++) {
                obinstream &obs = obss[i];
                while (!obs.eof()) {
                    obs >> vid >> msg;
                    if (msg.is_forward()) {
                        msg_num += out_ghost[vid].size();
                        for (NODETYPE neb:out_ghost[vid]) {
                            incoming_msgs[vid2idx[neb]].emplace_back(msg);
                        }
                    } else {
                        msg_num += in_ghost[vid].size();
                        for (NODETYPE neb:in_ghost[vid]) {
                            incoming_msgs[vid2idx[neb]].emplace_back(msg);
                        }
                    }
                }
            }
            return msg_num;
        }

        uint64_t bgn_compute() {
            uint64_t num_msgs = 0;
            vector<ibinstream> ibss(np);
            for (vertex &v:vertexes) {
                message msg;
                msg.level = v.level;
                broadcast_msg(v, msg, ibss);
                msg.set_backward();
                broadcast_msg(v, msg, ibss);
            }
            vector<obinstream> obss = exchange_bin_data(ibss);
            return unpack_msg_buf(obss);
        }

        uint64_t msg_compute() {
            uint64_t num_msgs = 0;
            vector<ibinstream> ibss(np);
            for (NODETYPE i = 0; i < vertexes.size(); i++) {
                vertex_msg_compute(vertexes[i], incoming_msgs[i], ibss);
                incoming_msgs[i].clear();
            }
            vector<obinstream> obss = exchange_bin_data(ibss);
            return unpack_msg_buf(obss);
        }

        void collect_index() {
            for (vertex &v:vertexes) {
                for (NODETYPE i = 0; i < v.ilb_cand.size(); i++) {
                    if (!v.fdv[v.ilb_cand[i]]) {
                        v.ilb.push_back(v.ilb_cand[i]);
                    }
                }
                v.ilb_cand.clear(), v.ilb_cand.shrink_to_fit();
                for (NODETYPE i = 0; i < v.olb_cand.size(); i++) {
                    if (!v.bdv[v.olb_cand[i]]) {
                        v.olb.push_back(v.olb_cand[i]);
                    }
                }
                v.olb_cand.clear(), v.olb_cand.shrink_to_fit();
            }
        }

    public:
        void run() {
            sync_graph(load_bin_graph());
            global_vnum = all_sum(vertexes.size());

            log_general("done! sync graph");

//            do some init things
            for (NODETYPE i = 0; i < vertexes.size(); i++) {
                vertexes[i].init(global_vnum);
                vid2idx[vertexes[i].id] = i;
            }
            incoming_msgs.resize(global_vnum);
            log_general("done! init")
            global_step_num = 0;
            uint64_t msg_num = bgn_compute();
            log_general("done! bgn_compute")
            global_step_num++;
            while (msg_num > 0) {
                msg_num = msg_compute();
                log_general("done! msg_compute")
                global_step_num++;
            }
            collect_index();
            cout << "dist reachability finished" << endl;
            int sum = 0;
            for (vertex &v:vertexes) {
                sum += v.ilb.size();
                sum += v.olb.size();
            }
            cout << "the index size is:" << sum << endl;

//            output_status();
        }

        void output_status() {
            ostringstream oss;
            oss << "====================graph===================" << endl;
            for (vertex &v:vertexes) {
                oss << v.id << " " << v.level << " in:{";
                for (NODETYPE n:v.inebM) {
                    oss << n << ",";
                }
                oss << "} out:{";
                for (NODETYPE n:v.onebM) {
                    oss << n << ",";
                }
                oss << "}" << endl;
            }

            oss << "====================ghost=================" << endl;
            for (auto &e:in_ghost) {
                oss << e.first << " ing{";
                for (auto &e1:e.second) {
                    oss << e1 << ",";
                }
                oss << "}" << endl;
            }

            for (auto &e:out_ghost) {
                oss << e.first << " oug{";
                for (auto &e1:e.second) {
                    oss << e1 << ",";
                }
                oss << "}" << endl;
            }

            oss << "====================index=================" << endl;
            for (vertex &v:vertexes) {
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
        vector<vertex> vertexes;
        unordered_map<NODETYPE, NODETYPE> vid2idx;
        unordered_map<NODETYPE, vector<NODETYPE >> in_ghost, out_ghost;
        vector<vector<message>> incoming_msgs;
    };
}
#endif //REACHABILITY_WORKERO1_H

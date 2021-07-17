#ifndef WORKER_HO4m1
#define WORKER_HO4m1

/**
 * this version of code implements the no batch version of Outer&Inner partial pruning algorithm
 *
 */

#include <vector>
#include <fstream>
#include "../utils/global.h"
#include <string>
#include "../utils/serialization.h"
#include "../utils/communication.h"
#include "../utils/Combiner.h"
#include "../utils/Aggregator.h"
#include "../utils/GraphIO.h"
#include <thread>
#include <sstream>
#include "../utils/hash_set.h"
#include "../utils/settings.h"

#include "../utils/butterfly.h"
//#include "../../butterfly/lib/graph_io.h"
//#include "../../butterfly/lib/butterfly.h"

#define NONE                 "\e[0m"
#define L_GREEN              "\e[1;32m"
using namespace std;
namespace O4m1 {
	
	NODETYPE vid2idx(NODETYPE vid) {
		return vid / np;
	}
	
	bool set_intersection(vector<NODETYPE> &a, vector<NODETYPE> &b) {
		int size_a = a.size();
		int size_b = b.size();
		int size_1 = 0;
		int size_2 = 0;
		while (size_1 < size_a && size_2 < size_b) {
			if (a[size_1] < b[size_2]) {
				size_1++;
			} else if (a[size_1] > b[size_2]) {
				size_2++;
			} else {
				return true; //contain same element
			}
		}
		return false;
	}
	
	class centralized_vertex {
	public:
		NODETYPE id, level;
		vector<NODETYPE> ineb, oneb;
	};
	
	class Vertex {
	public:
		Vertex() {}
		
		Vertex(centralized_vertex &v) {
			id = v.id;
			level = v.level;
			vector<bool> im(np, false), om(np, false);
			for (NODETYPE i:v.ineb)
				im[vid2workerid(i)] = true;
			for (NODETYPE i:v.oneb)
				om[vid2workerid(i)] = true;
			for (char i = 0; i < im.size(); i++) {
				if (im[i]) {
					this->im.push_back(i);
				}
			}
			this->im.shrink_to_fit();
			for (char i = 0; i < om.size(); i++) {
				if (om[i]) {
					this->om.push_back(i);
				}
			}
			this->om.shrink_to_fit();
		}
		
		
		vector<char> im, om;//in(out) neighbor machine
		vector<NODETYPE> iml, oml;//in(out) neighbor max level
		vector<NODETYPE> ilb, olb;
		
		koala::my_openadd_hashset iVisit, oVisit;
		
		NODETYPE id;
		NODETYPE level;
	};
	
	obinstream &operator>>(obinstream &obs, Vertex &v) {
		obs >> v.id >> v.level >> v.im >> v.om;
		return obs;
	}
	
	ibinstream &operator<<(ibinstream &ibs, const Vertex &v) {
		ibs << v.id << v.level << v.im << v.om;
		return ibs;
	}


//===========================================Batch Relevant Code====================
	int batch = 0;
	
	bool batch_first_supstep = true;
	double Bsize = 0;
	
	
	struct mirror_vertex {
		mirror_vertex() {}
		
		vector<NODETYPE> inBL, ouBL;
	};
	
	
	//store mirrors current batch index
	vector<mirror_vertex> mir;
	
	
	class Worker {
	public:
		Worker() : forward_gmsg(np), backward_gmsg(np) {}
		
		void preprocess() {
			//building vertex.im(om) data structure.
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
		
		
		inline void broadcast(Vertex &v, NODETYPE level, bool forward) {
			if (forward) {
				for (size_t i = 0; i < v.om.size(); ++i) {
					if (v.oml[i] >= batch_begin_lvl) {
						forward_gmsg[v.om[i]].emplace_back(v.id, level);
					}
				}
			} else {
				for (size_t i = 0; i < v.im.size(); ++i) {
					if (v.iml[i] >= batch_begin_lvl) {
						backward_gmsg[v.im[i]].emplace_back(v.id, level);
					}
				}
			}
		}
		
		
		void batch_begin_compute() {
			map<NODETYPE, NODETYPE>::iterator it = lvl_v_idx.lower_bound(batch_begin_lvl);
			for (; it != lvl_v_idx.end() && it->first < batch_end_lvl; ++it) {
				Vertex &v = vertexes[it->second];
				broadcast(v, inBatchLevel(v.level), true);
				broadcast(v, inBatchLevel(v.level), false);
			}
			
			Ibin_all_to_all_64(forward_gmsg);
			Ibin_all_to_all_64(backward_gmsg);
		}
		
		
		void batch_msg_compute() {
			vector<vector<pair<NODETYPE, NODETYPE >>> _forward_gmsg(np), _backward_gmsg(np);
			swap(forward_gmsg, _forward_gmsg);
			swap(backward_gmsg, _backward_gmsg);
			
			for (vector<pair<NODETYPE, NODETYPE>> &a:_forward_gmsg) {
				for (pair<NODETYPE, NODETYPE> msg:a) {
					NODETYPE msglevel = msg.second + batch_begin_lvl;
					for (NODETYPE idx:out_ghosts[msg.first]) {
						Vertex &v = vertexes[idx];
						if (v.level <= msglevel)continue;//pruning
						if (v.iVisit.find(msg.second))continue;//has visited
						if (v.iVisit.intersection(mir[msg.second].ouBL))continue;//prune by batch partial label
						
						if (v.level < batch_end_lvl) {//if it is in current batch.
							in_inc_label.emplace_back(v.level - batch_begin_lvl, msg.second);
						}
						
						v.iVisit.insert(msg.second);
						broadcast(v, msg.second, true);
					}
				}
			}
			
			_forward_gmsg.clear(), _forward_gmsg.shrink_to_fit();
			
			for (vector<pair<NODETYPE, NODETYPE>> &a:_backward_gmsg) {
				for (pair<NODETYPE, NODETYPE> msg:a) {
					NODETYPE msglevel = msg.second + batch_begin_lvl;
					for (NODETYPE idx:in_ghosts[msg.first]) {
						Vertex &v = vertexes[idx];
						if (v.level <= msglevel)continue;//pruning
						
						if (v.oVisit.find(msg.second))continue;//has visited
						if (v.oVisit.intersection(mir[msg.second].inBL))continue;//prune by batch partial label
						
						if (v.level < batch_end_lvl) {//in current batch
							ou_inc_label.emplace_back(v.level - batch_begin_lvl, msg.second);
						}
						
						v.oVisit.insert(msg.second);
						broadcast(v, msg.second, false);
					}
				}
			}
			_backward_gmsg.clear(), _backward_gmsg.shrink_to_fit();
			
			share_inc_Batch_reach_info();
			Ibin_all_to_all_64(forward_gmsg);
			Ibin_all_to_all_64(backward_gmsg);
		}
		
		void share_inc_Batch_reach_info() {
			vector<vector<pair<NODETYPE, NODETYPE >>> _in_inc_reach_info = Ibin_all_share_all(in_inc_label);
			vector<vector<pair<NODETYPE, NODETYPE >>> _ou_inc_reach_info = Ibin_all_share_all(ou_inc_label);
			for (auto &a:_in_inc_reach_info) {
				for (auto b:a) {
					mir[b.first].inBL.emplace_back(b.second);
				}
			}
			for (auto &a:_ou_inc_reach_info) {
				for (auto b:a) {
					mir[b.first].ouBL.emplace_back(b.second);
				}
			}
		}
		
		
		void postbatch() {
			for (NODETYPE idx = 0; idx < vertexes.size(); ++idx) {
				Vertex &v = vertexes[idx];
				vector<NODETYPE> &oVec = v.oVisit.release_data();
				vector<NODETYPE> tmpl;
				for (NODETYPE a:oVec) {
					if (a != 4294967295U) {
						if (!v.oVisit.intersection(mir[a].inBL))tmpl.emplace_back(a + batch_begin_lvl);
					}
				}
				v.oVisit.clear();
				sort(tmpl.begin(), tmpl.end());
				v.olb.insert(v.olb.end(), tmpl.begin(), tmpl.end());
				
				vector<NODETYPE> iVec = v.iVisit.release_data();
				tmpl.clear();
				for (NODETYPE a:iVec) {
					if (a != 4294967295U) {
						if (!v.iVisit.intersection(mir[a].ouBL))tmpl.emplace_back(a + batch_begin_lvl);
					}
				}
				v.iVisit.clear();
				sort(tmpl.begin(), tmpl.end());
				v.ilb.insert(v.ilb.end(), tmpl.begin(), tmpl.end());
			}
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
			if (me == MASTER_RANK) {
//                cout << "Input: " << setting::inputFileDirectory << endl;
				PrintTimer("Graph Load Time", WORKER_TIMER);
			}
			worker_barrier();
			//=========================================================
			init_timers();
			ResetTimer(WORKER_TIMER);
			runtime = get_current_time();
			ResetTimer(TRANSFER_TIMER);
			
			
			global_step_num = 0;
			
			uint64_t step_gmsg_num = 0;
			uint64_t global_gmsg_num = 0;
			
			//initializations
			batch = 0;
			batch_begin_lvl = 0;
			ResetTimer(PRE_BATCH_TIMER);
			
			//prebatch goes here
			ResetTimer(BATCH_TIMER);
			batch_end_lvl = batch_begin_lvl + global_vnum;
			StartTimer(PRE_BATCH_TIMER);
			mir.resize(batch_end_lvl - batch_begin_lvl);
			StopTimer(PRE_BATCH_TIMER);
			
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
					step_gmsg_num += forward_gmsg[i].size();
					step_gmsg_num += backward_gmsg[i].size();
				}
				step_gmsg_num = all_sum_LL(step_gmsg_num);
				global_gmsg_num += step_gmsg_num;
				
				StopTimer(SUPER_STEP_TIMER);
				if (step_gmsg_num == 0)break;
				batch_first_supstep = false;
			}
			//postbatch goes here
			postbatch();
			
			
			StopTimer(WORKER_TIMER);
			runtime = get_current_time() - runtime;
//            if (me == MASTER_RANK) {
//                record("runtime is %f", runtime)
//                cout << "Total gmsgs#" << global_gmsg_num << endl;
//            }
			
			worker_barrier();

//        dump_partition(params.output_path);
			{
				uint64_t idxsize = 0;
				for (Vertex &v:vertexes) {
					idxsize += v.ilb.size();
					idxsize += v.olb.size();
				}
				idxsize = master_sum_LL(idxsize);
				if (me == MASTER_RANK) record(
						"runtime=%f, transfertime=%f, idx_size=%lu, #gmsg=%lu",
						runtime,
						get_timer(TRANSFER_TIMER),
						idxsize,
						global_gmsg_num)
			}
//            worker_barrier();
//            diff();
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
		
		void diff() {
			string basename = "/data/junhzhan/drlData/citeseer/citeseer.VBDuL.parts/citeseer";
			vector<vector<NODE_TYPE >> graph, rgraph;
			vector<NODE_TYPE> level;
			load_VBDuL_graph_parts(graph, rgraph, level, basename);
			double time_now = get_current_time();
			pair<vector<vector<NODE_TYPE >>, vector<vector<NODE_TYPE >>> idx =
					butterfly::butterfly(graph, rgraph, level);
			cout << "time" << get_current_time() - time_now << endl;
			ostringstream oss;
			for (NODETYPE level = 0; level < vertexes.size(); level++) {
				size_t i = lvl_v_idx[level];
				if (vertexes[i].ilb.size() != idx.first[i].size() || vertexes[i].olb.size() != idx.second[i].size()) {
					oss << "diff of (v,l) (" << i << "," << level << ") ";
					if (vertexes[i].ilb.size() != idx.first[i].size()) {
						oss << " drl.ilb: ";
						for (auto a:vertexes[i].ilb)oss << a << ",";
						oss << " bty.lin: ";
						for (auto a:idx.first[i])oss << a << ",";
					}
					if (vertexes[i].olb.size() != idx.second[i].size()) {
						oss << " drl.olb: ";
						for (auto a:vertexes[i].olb)oss << a << ",";
						oss << " bty.lou: ";
						for (auto a:idx.second[i])oss << a << ",";
					}
					oss << endl;
					break;
				} else {
					for (int j = 0; j < vertexes[i].ilb.size(); ++j) {
						assert(vertexes[i].ilb[j] == idx.first[i][j]);
					}
					for (int j = 0; j < vertexes[i].olb.size(); ++j) {
						assert(vertexes[i].olb[j] == idx.second[i][j]);
					}
				}
				
			}
			cout << oss.str();
		}
	
	private:
		vector<Vertex> vertexes;
		hash_map<NODETYPE, vector<NODETYPE>> out_ghosts, in_ghosts; //vid->nbs's idx
		
		vector<vector<pair<NODETYPE, NODETYPE >>> forward_gmsg, backward_gmsg;
		
		vector<pair<NODETYPE, NODETYPE>> in_inc_label, ou_inc_label;
		
		map<NODETYPE, NODETYPE> lvl_v_idx;//level->pos in the vertexes
	};
	
	
	void build_indexes() {
//        log_general("O4m1 building indexes.")
		Worker worker;
		worker.run();
	}
	
}
#endif //WORKER_HO4m1

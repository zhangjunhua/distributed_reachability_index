/**
 *This version of Algorithm, Contains the following optimizations
 * 1. Dual-BFS
 * 2. Divide BFSs into Batches, to alleviate the memory pressure
 * * Load Balance and MSG Reduction with ghosts.
 * 3. No Pruning with Partial Labels.
 */
#ifndef WORKER_HO2a
#define WORKER_HO2a

#include <unistd.h>
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
#include "VertexO2a.h"

#define NONE                 "\e[0m"
#define L_GREEN              "\e[1;32m"
using namespace std;
namespace O2a {


//===========================================Batch Relevant Code====================
	int batch = 0;
//an array define size of each batch4
	vector<int> batch_size;
	bool batch_first_supstep = true;
	NODETYPE Bsize = 0;
	
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
		Worker() : del_forward_gmsg(np), del_backward_gmsg(np), add_forward_gmsg(np), add_backward_gmsg(np) {}
		
		
		inline void broadcast(Vertex &v, NODETYPE level, bool add, bool forward) {
//            printf("[broadcast] %u broadcast [lvl:%u,add:%d,fwd:%d]\n", v.level, level + batch_begin_lvl, add, forward);
			if (add) {
				if (forward) {
					for (size_t i = 0; i < v.om.size(); ++i) {
						add_forward_gmsg[v.om[i]].emplace_back(v.id, level);
					}
				} else {
					for (size_t i = 0; i < v.im.size(); ++i) {
						add_backward_gmsg[v.im[i]].emplace_back(v.id, level);
					}
				}
			} else {
				if (forward) {
					for (size_t i = 0; i < v.om.size(); ++i) {
						del_forward_gmsg[v.om[i]].emplace_back(v.id, level);
					}
				} else {
					for (size_t i = 0; i < v.im.size(); ++i) {
						del_backward_gmsg[v.im[i]].emplace_back(v.id, level);
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
					for (NODETYPE idx:out_ghosts[msg.first]) {
						Vertex &v = vertexes[idx];
						if (v.ist.find(msg.second) != v.ist.end() && !v.ist[msg.second])continue;//has beed deleted
						v.ist[msg.second] = false;
						status_update(idx);
						broadcast(v, msg.second, false, true);
					}
				}
			}
			_del_forward_gmsg.clear();
			for (vector<pair<NODETYPE, NODETYPE>> &a:_del_backward_gmsg) {
				for (pair<NODETYPE, NODETYPE> msg:a) {
					for (NODETYPE idx:in_ghosts[msg.first]) {
						Vertex &v = vertexes[idx];
						if (v.ost.find(msg.second) != v.ost.end() && !v.ost[msg.second])continue;//has beed deleted
						v.ost[msg.second] = false;
						status_update(idx);
						broadcast(v, msg.second, false, false);
					}
				}
			}
			_del_backward_gmsg.clear();
			for (vector<pair<NODETYPE, NODETYPE>> &a:_add_forward_gmsg) {
				for (pair<NODETYPE, NODETYPE> msg:a) {
					NODETYPE msglevel = msg.second + batch_begin_lvl;
					for (NODETYPE idx:out_ghosts[msg.first]) {
						Vertex &v = vertexes[idx];
						if (v.ist.find(msg.second) != v.ist.end())continue;//has been added or deleted
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
			for (vector<pair<NODETYPE, NODETYPE>> &a:_add_backward_gmsg) {
				for (pair<NODETYPE, NODETYPE> msg:a) {
					NODETYPE msglevel = msg.second + batch_begin_lvl;
					for (NODETYPE idx:in_ghosts[msg.first]) {
						Vertex &v = vertexes[idx];
						if (v.ost.find(msg.second) != v.ost.end())continue;//has been added or deleted
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
			
			Ibin_all_to_all_64(del_forward_gmsg);
			Ibin_all_to_all_64(del_backward_gmsg);
			Ibin_all_to_all_64(add_forward_gmsg);
			Ibin_all_to_all_64(add_backward_gmsg);
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
			ResetTimer(TRANSFER_TIMER);
			
			//supersteps
			global_step_num = 0;
			uint64_t step_gmsg_num = 0, step_dgmsg_num = 0;
			uint64_t bat_gmsg_num = 0, bat_dgmsg_num = 0;
			
			uint64_t global_gmsg_num = 0, global_dgmsg_num = 0;
			//initializations
			init_batch_size();
			batch = 0;
			Bsize = 1;
			batch_begin_lvl = 0;
			ResetTimer(PRE_BATCH_TIMER);
			abc:
			while (true) {
				//prebatch goes here
				ResetTimer(BATCH_TIMER);
				batch_end_lvl = batch_begin_lvl + Bsize;
				StartTimer(PRE_BATCH_TIMER);
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
					float pct = ((double) step_dgmsg_num) / ((double) step_gmsg_num) * 100;
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
//				if (me == MASTER_RANK) {
//					printf(L_GREEN
//					       "Batch %d of [%d,%d) with gmsg#%llu dmsg#%llu(%f\%) in sb(%d) done in %f seconds.\n" NONE,
//					       batch, batch_begin_lvl, batch_end_lvl, bat_gmsg_num, bat_dgmsg_num, pct,
//					       share_before_batch,
//					       get_timer(BATCH_TIMER));
//				}
				
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
				Bsize = Bsize * 1.01 + 1;
				if (Bsize > setting::max_Bsize)
					Bsize = setting::max_Bsize;
				if (me == MASTER_RANK) {
					if ((get_current_time() - runtime) > setting::runtimelimit) {
						record("runtime is %f", 1000000);
						exit(-1);
					}
				}
			}
			
			
			StopTimer(WORKER_TIMER);
			runtime = get_current_time() - runtime;
//			if (me == MASTER_RANK) record("runtime is %f", runtime)
//			if (me == MASTER_RANK) {
//				float pct = ((double) global_dgmsg_num) / ((double) global_gmsg_num) * 100;
//				cout << "Total gmsgs#" << global_gmsg_num << " dgmsg#" << global_dgmsg_num << "(" << pct << "%)"
//				     << endl;
//			}
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
				if (me == MASTER_RANK) record(
						"runtime=%f, transfertime=%f, idx_size=%lu, #gmsg=%lu, dgmsg#=%lu, pct=%f",
						runtime,
						get_timer(TRANSFER_TIMER),
						idxsize,
						global_gmsg_num,
						global_dgmsg_num,
						((double) global_dgmsg_num) / ((double) global_gmsg_num) * 100)
			}
			StopTimer(WORKER_TIMER);
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
		
		map<NODETYPE, NODETYPE> lvl_v_idx;//level->pos in the vertexes
	};
	
	void report_start() {
		string pipename = "/home/junhzhan/pip_reach";
		FILE *f = fopen(pipename.c_str(), "w");
		if (!f) {
			printf("open pip file %s failed, exit.\n", pipename.c_str());
			exit(-1);
		}
		int pid = getpid();
		
		fprintf(f, "%s %d %d start\n", setting::inputFileDirectory.c_str(), me, pid);
		
		fclose(f);
	}
	
	void report_end() {
		string pipename = "/home/junhzhan/pip_reach";
		FILE *f = fopen(pipename.c_str(), "w");
		if (!f) {
			printf("open pip file %s failed, exit.\n", pipename.c_str());
			exit(-1);
		}
		int pid = getpid();
		
		fprintf(f, "%s %d %d end\n", setting::inputFileDirectory.c_str(), me, pid);
		
		fclose(f);
	}
	
	void build_indexes() {
		Worker worker;
//        report_start();
		worker.run();
//        report_end();
	}
	
}
#endif //WORKER_HO2a
//Acknowledgements: this code is implemented by referencing pregel-mpi (https://code.google.com/p/pregel-mpi/) by Chuntao Hong.

#ifndef COMMUNICATION_H
#define COMMUNICATION_H

#include <mpi.h>
#include "time.h"
#include <iostream>
#include "serialization.h"
#include "global.h"

#define ST (printf("%s(%d) rank#%d:",_FILE_,_LINE_,_my_rank),printf)

//============================================
//Allreduce
int all_sum(int my_copy) {
	int tmp;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
	return tmp;
}

unsigned long long master_sum_LL(unsigned long long my_copy) {
	unsigned long long tmp = 0;
	MPI_Reduce(&my_copy, &tmp, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, MASTER_RANK,
	           MPI_COMM_WORLD);
	return tmp;
}

long long all_sum_LL(unsigned long long my_copy) {
	unsigned long long tmp = 0;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM,
	              MPI_COMM_WORLD);
	return tmp;
}

char all_bor(char my_copy) {
	char tmp;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_BOR, MPI_COMM_WORLD);
	return tmp;
}

/*
 bool all_lor(bool my_copy){
 bool tmp;
 MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_LOR, MPI_COMM_WORLD);
 return tmp;
 }
 */

//============================================
//char-level send/recv
void pregel_send(void *buf, int size, int dst) {
	MPI_Send(buf, size, MPI_CHAR, dst, 0, MPI_COMM_WORLD);
}

void pregel_recv(void *buf, int size, int src) {
	MPI_Recv(buf, size, MPI_CHAR, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

//============================================
//binstream-level send/recv
void send_ibinstream(ibinstream &m, int dst) {
	size_t size = m.size();
	pregel_send(&size, sizeof(size_t), dst);
	pregel_send(m.get_buf(), m.size(), dst);
}

obinstream recv_obinstream(int src) {
	size_t size;
	pregel_recv(&size, sizeof(size_t), src);
	char *buf = new char[size];
	pregel_recv(buf, size, src);
	return obinstream(buf, size);
}

template<class T>
void all_to_all_Ibcast(vector<T> &to_send, vector<T> &to_get) {
	StartTimer(COMMUNICATION_TIMER);
	
	StartTimer(SERIALIZATION_TIMER);
	ibinstream m;
	m << to_send;
	StopTimer(SERIALIZATION_TIMER);
	
	StartTimer(TRANSFER_TIMER);
	char *recv_bufs[np];
	int bufsize[np];
	MPI_Request mpi_sendrequests[np];
	MPI_Request mpi_recvrequests[np];
	MPI_Status statuses[np];
	/* Algorithm DESC:
	 * 1.issue non-blocking sending to each process.
	 * 2.Probe messages from each process.
	 * 3.issue non-blocking recving from each process.
	 * 4.wait till all send/recv request finish.
	 * 5.deserilize the data
	 */
	for (int dest = 0; dest < np; dest++) { //1.issue non-blocking send request
		if (me != dest)
			MPI_Isend(m.get_buf(), m.size(), MPI_CHAR, dest, 0, MPI_COMM_WORLD,
			          &mpi_sendrequests[dest]);
		else
			mpi_sendrequests[me] = MPI_REQUEST_NULL;
	}
	
	MPI_Status status;
	for (int src = 0; src < np; src++) {
		/* 2.probe size and allocate memory
		 * 3.issue recv request
		 */
		if (me != src) {
			MPI_Probe(src, 0, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &bufsize[src]);
			recv_bufs[src] = new char[bufsize[src]];
			MPI_Irecv(recv_bufs[src], bufsize[src], MPI_CHAR, src, 0,
			          MPI_COMM_WORLD, &mpi_recvrequests[src]);
		} else
			mpi_recvrequests[me] = MPI_REQUEST_NULL;
	}
	/*
	 * 4. wait till all send/recv request finish
	 */
	MPI_Waitall(np, mpi_sendrequests, statuses);
	MPI_Waitall(np, mpi_recvrequests, statuses);
	StopTimer(TRANSFER_TIMER);
	/*
	 * 5.deserilize the data
	 */
	StartTimer(SERIALIZATION_TIMER);
	size_t size;
	size_t oldtempsize;
	for (int src = 0; src < np; ++src) {
		if (me != src) {
			//um is responsible for deallocate the recv_bufs[dist_reach]
			obinstream um(recv_bufs[src], bufsize[src]);
			um >> size;
			
			oldtempsize = to_get.size();
			to_get.resize(to_get.size() + size);
			
			for (int i = oldtempsize; i < to_get.size(); ++i) {
				um >> to_get[i];
			}
		}
	}
	StopTimer(SERIALIZATION_TIMER);
	StopTimer(COMMUNICATION_TIMER);
}

//============================================
//obj-level send/recv
template<class T>
void send_data(const T &data, int dst) {
	ibinstream m;
	m << data;
	send_ibinstream(m, dst);
}

template<class T>
T recv_data(int src) {
	obinstream um = recv_obinstream(src);
	T data;
	um >> data;
	return data;
}

//============================================
//all-to-all
template<class T>
void all_to_all(std::vector<T> &to_exchange) {
	StartTimer(COMMUNICATION_TIMER);
	//for each to_exchange[i]
	//        send out *to_exchange[i] to i
	//        save received data in *to_exchange[i]
	for (int i = 0; i < np; i++) {
		int partner = (i - me + np) % np;
		if (me != partner) {
			if (me < partner) {
				StartTimer(SERIALIZATION_TIMER);
				//send
				ibinstream m;
				m << to_exchange[partner];
				StopTimer(SERIALIZATION_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_ibinstream(m, partner);
				StopTimer(TRANSFER_TIMER);
				//receive
				StartTimer(TRANSFER_TIMER);
				obinstream um = recv_obinstream(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				um >> to_exchange[partner];
				StopTimer(SERIALIZATION_TIMER);
			} else {
				StartTimer(TRANSFER_TIMER);
				//receive
				obinstream um = recv_obinstream(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				T received;
				um >> received;
				//send
				ibinstream m;
				m << to_exchange[partner];
				StopTimer(SERIALIZATION_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_ibinstream(m, partner);
				StopTimer(TRANSFER_TIMER);
				to_exchange[partner] = received;
			}
		}
	}
	StopTimer(COMMUNICATION_TIMER);
}

vector<obinstream> exchange_bin_data(vector<ibinstream> &ibss) {
	char *recv_bufs[np];
	int recv_size[np];
	MPI_Request mpi_sendrequests[np];
	MPI_Request mpi_recvrequests[np];
	MPI_Status statuses[np];
	
	StartTimer(TRANSFER_TIMER);
	for (int dest = 0; dest < np; dest++) {
		//send requests
		if (me != dest) {
			MPI_Isend(ibss[dest].get_buf(), ibss[dest].size(),
			          MPI_CHAR, dest, 0, MPI_COMM_WORLD, &mpi_sendrequests[dest]);
		} else
			mpi_sendrequests[dest] = MPI_REQUEST_NULL;
	}
	MPI_Status status;
	for (int src = 0; src < np; src++) {
		//3,probe size, allocate memory and recv request
		if (me != src) {
			MPI_Probe(src, 0, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &recv_size[src]);
			recv_bufs[src] = new char[recv_size[src]];
			MPI_Irecv(recv_bufs[src], recv_size[src], MPI_CHAR, src, 0,
			          MPI_COMM_WORLD, &mpi_recvrequests[src]);
		} else {
			mpi_recvrequests[src] = MPI_REQUEST_NULL;
			recv_bufs[src] = new char[ibss[src].size()];
			recv_size[src] = ibss[src].size();
			memcpy(recv_bufs[src], ibss[src].get_buf(), ibss[src].size());
		}
	}
	
	//4.wait till all send/recv request finish
	MPI_Waitall(np, mpi_sendrequests, statuses);
	MPI_Waitall(np, mpi_recvrequests, statuses);
	StopTimer(TRANSFER_TIMER);
	vector<obinstream> obss(np);
	for (int src = 0; src < np; src++) {
		ibss[src].clear_buf();
		obss[src].set_buf(recv_bufs[src], recv_size[src]);
	}
	return obss;
}


/**
 * no serilization version of communication.
 * @tparam T
 * @param to_exchange
 */
template<typename T>
void Ibin_all_to_all_64(std::vector<vector<T>> &to_exchange) {
	MPI_Request mpi_send_rq[np];
	MPI_Request mpi_recv_rq[np];
	assert(sizeof(T) == 8);
	vector<vector<T>> recv_buf(np);
	StartTimer(TRANSFER_TIMER);
	//issue send req
	for (int dst = 0; dst < np; ++dst) {
		if (dst != me) {
			MPI_Isend(to_exchange[dst].data(), to_exchange[dst].size(),
			          MPI_UINT64_T, dst, 0, MPI_COMM_WORLD, &mpi_send_rq[dst]);
		} else
			mpi_send_rq[dst] = MPI_REQUEST_NULL;
	}
	//probe incoming msg, then allocate memory and issue recv req
	for (int src = 0; src < np; ++src) {
		if (src != me) {
			MPI_Status st;
			int recv_size;
			MPI_Probe(src, 0, MPI_COMM_WORLD, &st);
			MPI_Get_count(&st, MPI_UINT64_T, &recv_size);
			recv_buf[src].resize(recv_size);
			MPI_Irecv(recv_buf[src].data(), recv_buf[src].size(), MPI_UINT64_T, src, 0,
			          MPI_COMM_WORLD, &mpi_recv_rq[src]);
		} else
			mpi_recv_rq[src] = MPI_REQUEST_NULL;
	}
	//wait for complete of send req and free space
	for (int dst = 0; dst < np - 1; ++dst) {
		MPI_Status st;
		int idx;
		MPI_Waitany(np, mpi_send_rq, &idx, &st);
		to_exchange[idx].clear();
		to_exchange[idx].shrink_to_fit();
	}
	for (int src = 0; src < np - 1; ++src) {
		MPI_Status st;
		int idx = 0;
		MPI_Waitany(np, mpi_recv_rq, &idx, &st);
		swap(to_exchange[idx], recv_buf[idx]);
	}
	StopTimer(TRANSFER_TIMER);
}

/**
 * called by every nodes,
 * every nodes share the binary data out to other nodes.
 * @tparam T
 * @param out data to be shared, the content of out is cleared.
 * @return data shared by other nodes
 */
template<typename T>
vector<vector<T>> Ibin_all_share_all(vector<T> &out) {
	MPI_Request mpi_send_rq[np];
	MPI_Request mpi_recv_rq[np];
	assert(sizeof(T) == 8);
	vector<vector<T>> recv_buf(np);
	StartTimer(TRANSFER_TIMER);
	//issue send req
	for (int dst = 0; dst < np; ++dst) {
		if (dst != me) {
			MPI_Isend(out.data(), out.size(), MPI_UINT64_T, dst, 0, MPI_COMM_WORLD, &mpi_send_rq[dst]);
		} else
			mpi_send_rq[dst] = MPI_REQUEST_NULL;
	}
	//probe incoming msg, then allocate memory and issue recv req
	for (int src = 0; src < np; ++src) {
		if (src != me) {
			MPI_Status st;
			int recv_size;
			MPI_Probe(src, 0, MPI_COMM_WORLD, &st);
			MPI_Get_count(&st, MPI_UINT64_T, &recv_size);
			recv_buf[src].resize(recv_size);
			MPI_Irecv(recv_buf[src].data(), recv_buf[src].size(), MPI_UINT64_T, src, 0,
			          MPI_COMM_WORLD, &mpi_recv_rq[src]);
		} else
			mpi_recv_rq[src] = MPI_REQUEST_NULL;
	}
	//wait for complete of send req and free space
	MPI_Waitall(np, mpi_send_rq, MPI_STATUSES_IGNORE);
	MPI_Waitall(np, mpi_recv_rq, MPI_STATUSES_IGNORE);
	StopTimer(TRANSFER_TIMER);
	swap(recv_buf[me], out);
	
	return recv_buf;
}

//============================================
//non-blocking all-to-all
template<class T>
void Iall_to_all(std::vector<T> &to_exchange) {
	StartTimer(COMMUNICATION_TIMER);
	//for each to_exchange[i]
	//        send out *to_exchange[i] to i
	//        save received data in *to_exchange[i]
	
	ibinstream send_bufs[np];
	char *recv_bufs[np];
	int recv_size[np];
	MPI_Request mpi_sendrequests[np];
	MPI_Request mpi_recvrequests[np];
	MPI_Status statuses[np];
	/*
	 * Algorithm:
	 * 1,serilization
	 * 2,send request
	 * 3,probe size, allocate memory and recv request
	 * 4,wait till all send/recv request finish.
	 * 5,deserilize the data
	 */
	StartTimer(SERIALIZATION_TIMER);
	for (int dest = 0; dest < np; dest++) {
		//serilization and send requests
		if (me != dest) {
			send_bufs[dest] << to_exchange[dest];
			MPI_Isend(send_bufs[dest].get_buf(), send_bufs[dest].size(),
			          MPI_CHAR, dest, 0, MPI_COMM_WORLD, &mpi_sendrequests[dest]);
		} else
			mpi_sendrequests[dest] = MPI_REQUEST_NULL;
	}
	StopTimer(SERIALIZATION_TIMER);
	StartTimer(TRANSFER_TIMER);
	MPI_Status status;
	for (int src = 0; src < np; src++) {
		//3,probe size, allocate memory and recv request
		if (me != src) {
			MPI_Probe(src, 0, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &recv_size[src]);
			recv_bufs[src] = new char[recv_size[src]];
			MPI_Irecv(recv_bufs[src], recv_size[src], MPI_CHAR, src, 0,
			          MPI_COMM_WORLD, &mpi_recvrequests[src]);
		} else
			mpi_recvrequests[src] = MPI_REQUEST_NULL;
	}
	
	//4.wait till all send/recv request finish
	MPI_Waitall(np, mpi_sendrequests, statuses);
	MPI_Waitall(np, mpi_recvrequests, statuses);
	StopTimer(TRANSFER_TIMER);
	/*
	 * 5.deserilize the data
	 */
	StartTimer(SERIALIZATION_TIMER);
	for (int src = 0; src < np; ++src) {
		if (me != src) {
			obinstream um(recv_bufs[src], recv_size[src]);
			um >> to_exchange[src];
		}
	}
	StopTimer(SERIALIZATION_TIMER);
	
	StopTimer(COMMUNICATION_TIMER);
}

template<class T, class T1>
void all_to_all_cat(std::vector<T> &to_exchange1,
                    std::vector<T1> &to_exchange2) {
	StartTimer(COMMUNICATION_TIMER);
	//for each to_exchange[i]
	//        send out *to_exchange[i] to i
	//        save received data in *to_exchange[i]
	for (int i = 0; i < np; i++) {
		int partner = (i - me + np) % np;
		if (me != partner) {
			if (me < partner) {
				StartTimer(SERIALIZATION_TIMER);
				//send
				ibinstream m;
				m << to_exchange1[partner];
				m << to_exchange2[partner];
				StopTimer(SERIALIZATION_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_ibinstream(m, partner);
				StopTimer(TRANSFER_TIMER);
				//receive
				StartTimer(TRANSFER_TIMER);
				obinstream um = recv_obinstream(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				um >> to_exchange1[partner];
				um >> to_exchange2[partner];
				StopTimer(SERIALIZATION_TIMER);
			} else {
				StartTimer(TRANSFER_TIMER);
				//receive
				obinstream um = recv_obinstream(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				T received1;
				T1 received2;
				um >> received1;
				um >> received2;
				//send
				ibinstream m;
				m << to_exchange1[partner];
				m << to_exchange2[partner];
				StopTimer(SERIALIZATION_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_ibinstream(m, partner);
				StopTimer(TRANSFER_TIMER);
				to_exchange1[partner] = received1;
				to_exchange2[partner] = received2;
			}
		}
	}
	StopTimer(COMMUNICATION_TIMER);
}

template<class T, class T1>
void Iall_to_all_cat(std::vector<T> &to_exchange1,
                     std::vector<T1> &to_exchange2) {
	
	StartTimer(COMMUNICATION_TIMER);
	//for each to_exchange[i]
	//        send out *to_exchange[i] to i
	//        save received data in *to_exchange[i]
	
	ibinstream send_bufs[np];
	char *recv_bufs[np];
	int recv_size[np];
	MPI_Request mpi_sendrequests[np];
	MPI_Request mpi_recvrequests[np];
	MPI_Status statuses[np];
	/*
	 * Algorithm:
	 * 1,serilization
	 * 2,send request
	 * 3,probe size, allocate memory and recv request
	 * 4,wait till all send/recv request finish.
	 * 5,deserilize the data
	 */
	StartTimer(SERIALIZATION_TIMER);
	for (int dest = 0; dest < np; dest++) {
		//serilization and send requests
		if (me != dest) {
			send_bufs[dest] << to_exchange1[dest];
			send_bufs[dest] << to_exchange2[dest];
			
			MPI_Isend(send_bufs[dest].get_buf(), send_bufs[dest].size(),
			          MPI_CHAR, dest, 0, MPI_COMM_WORLD, &mpi_sendrequests[dest]);
		} else
			mpi_sendrequests[dest] = MPI_REQUEST_NULL;
	}
	StopTimer(SERIALIZATION_TIMER);
	StartTimer(TRANSFER_TIMER);
	MPI_Status status;
	for (int src = 0; src < np; src++) {
		//3,probe size, allocate memory and recv request
		if (me != src) {
			MPI_Probe(src, 0, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &recv_size[src]);
			recv_bufs[src] = new char[recv_size[src]];
			MPI_Irecv(recv_bufs[src], recv_size[src], MPI_CHAR, src, 0,
			          MPI_COMM_WORLD, &mpi_recvrequests[src]);
		} else
			mpi_recvrequests[src] = MPI_REQUEST_NULL;
	}
	
	//4.wait till all send/recv request finish
	MPI_Waitall(np, mpi_sendrequests, statuses);
	MPI_Waitall(np, mpi_recvrequests, statuses);
	StopTimer(TRANSFER_TIMER);
	/*
	 * 5.deserilize the data
	 */
	StartTimer(SERIALIZATION_TIMER);
	for (int src = 0; src < np; ++src) {
		if (me != src) {
			obinstream um(recv_bufs[src], recv_size[src]);
			um >> to_exchange1[src];
			um >> to_exchange2[src];
		}
	}
	StopTimer(SERIALIZATION_TIMER);
	StopTimer(COMMUNICATION_TIMER);
}


template<class T, class T1, class T2>
void all_to_all_cat(std::vector<T> &to_exchange1, std::vector<T1> &to_exchange2,
                    std::vector<T2> &to_exchange3) {
	StartTimer(COMMUNICATION_TIMER);
	//for each to_exchange[i]
	//        send out *to_exchange[i] to i
	//        save received data in *to_exchange[i]
	for (int i = 0; i < np; i++) {
		int partner = (i - me + np) % np;
		if (me != partner) {
			if (me < partner) {
				StartTimer(SERIALIZATION_TIMER);
				//send
				ibinstream m;
				m << to_exchange1[partner];
				m << to_exchange2[partner];
				m << to_exchange3[partner];
				StopTimer(SERIALIZATION_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_ibinstream(m, partner);
				StopTimer(TRANSFER_TIMER);
				//receive
				StartTimer(TRANSFER_TIMER);
				obinstream um = recv_obinstream(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				um >> to_exchange1[partner];
				um >> to_exchange2[partner];
				um >> to_exchange3[partner];
				StopTimer(SERIALIZATION_TIMER);
			} else {
				StartTimer(TRANSFER_TIMER);
				//receive
				obinstream um = recv_obinstream(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				T received1;
				T1 received2;
				T2 received3;
				um >> received1;
				um >> received2;
				um >> received3;
				//send
				ibinstream m;
				m << to_exchange1[partner];
				m << to_exchange2[partner];
				m << to_exchange3[partner];
				StopTimer(SERIALIZATION_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_ibinstream(m, partner);
				StopTimer(TRANSFER_TIMER);
				to_exchange1[partner] = received1;
				to_exchange2[partner] = received2;
				to_exchange3[partner] = received3;
			}
		}
	}
	StopTimer(COMMUNICATION_TIMER);
}

template<class T, class T1, class T2>
void Iall_to_all_cat(std::vector<T> &to_exchange1,
                     std::vector<T1> &to_exchange2, std::vector<T2> &to_exchange3) {
	
	StartTimer(COMMUNICATION_TIMER);
	//for each to_exchange[i]
	//        send out *to_exchange[i] to i
	//        save received data in *to_exchange[i]
	
	ibinstream send_bufs[np];
	char *recv_bufs[np];
	int recv_size[np];
	MPI_Request mpi_sendrequests[np];
	MPI_Request mpi_recvrequests[np];
	MPI_Status statuses[np];
	/*
	 * Algorithm:
	 * 1,serilization
	 * 2,send request
	 * 3,probe size, allocate memory and recv request
	 * 4,wait till all send/recv request finish.
	 * 5,deserilize the data
	 */
	StartTimer(SERIALIZATION_TIMER);
	for (int dest = 0; dest < np; dest++) {
		//serilization and send requests
		if (me != dest) {
			send_bufs[dest] << to_exchange1[dest];
			send_bufs[dest] << to_exchange2[dest];
			send_bufs[dest] << to_exchange3[dest];
			
			MPI_Isend(send_bufs[dest].get_buf(), send_bufs[dest].size(),
			          MPI_CHAR, dest, 0, MPI_COMM_WORLD, &mpi_sendrequests[dest]);
		} else
			mpi_sendrequests[dest] = MPI_REQUEST_NULL;
	}
	StopTimer(SERIALIZATION_TIMER);
	StartTimer(TRANSFER_TIMER);
	MPI_Status status;
	for (int src = 0; src < np; src++) {
		//3,probe size, allocate memory and recv request
		if (me != src) {
			MPI_Probe(src, 0, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_CHAR, &recv_size[src]);
			recv_bufs[src] = new char[recv_size[src]];
			MPI_Irecv(recv_bufs[src], recv_size[src], MPI_CHAR, src, 0,
			          MPI_COMM_WORLD, &mpi_recvrequests[src]);
		} else
			mpi_recvrequests[src] = MPI_REQUEST_NULL;
	}
	
	//4.wait till all send/recv request finish
	MPI_Waitall(np, mpi_sendrequests, statuses);
	MPI_Waitall(np, mpi_recvrequests, statuses);
	StopTimer(TRANSFER_TIMER);
	/*
	 * 5.deserilize the data
	 */
	StartTimer(SERIALIZATION_TIMER);
	for (int src = 0; src < np; ++src) {
		if (me != src) {
			obinstream um(recv_bufs[src], recv_size[src]);
			um >> to_exchange1[src];
			um >> to_exchange2[src];
			um >> to_exchange3[src];
		}
	}
	StopTimer(SERIALIZATION_TIMER);
	StopTimer(COMMUNICATION_TIMER);
}

template<class T, class T1>
void all_to_all(vector<T> &to_send, vector<T1> &to_get) {
	StartTimer(COMMUNICATION_TIMER);
	//for each to_exchange[i]
	//        send out *to_exchange[i] to i
	//        save received data in *to_exchange[i]
	for (int i = 0; i < np; i++) {
		int partner = (i - me + np) % np;
		if (me != partner) {
			if (me < partner) {
				StartTimer(SERIALIZATION_TIMER);
				//send
				ibinstream m;
				m << to_send[partner];
				StopTimer(SERIALIZATION_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_ibinstream(m, partner);
				StopTimer(TRANSFER_TIMER);
				//receive
				StartTimer(TRANSFER_TIMER);
				obinstream um = recv_obinstream(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				um >> to_get[partner];
				StopTimer(SERIALIZATION_TIMER);
			} else {
				StartTimer(TRANSFER_TIMER);
				//receive
				obinstream um = recv_obinstream(partner);
				StopTimer(TRANSFER_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				T1 received;
				um >> received;
				//send
				ibinstream m;
				m << to_send[partner];
				StopTimer(SERIALIZATION_TIMER);
				StartTimer(TRANSFER_TIMER);
				send_ibinstream(m, partner);
				StopTimer(TRANSFER_TIMER);
				to_get[partner] = received;
			}
		}
	}
	StopTimer(COMMUNICATION_TIMER);
}

//============================================
//scatter
template<class T>
void masterScatter(vector<T> &to_send) { //scatter
	StartTimer(COMMUNICATION_TIMER);
	int *sendcounts = new int[np]();
	int recvcount = 0;
	int *sendoffset = new int[np]();
	
	ibinstream m;
	StartTimer(SERIALIZATION_TIMER);
	int size = 0;
	for (int i = 0; i < np; i++) {
		if (i == me) {
			sendcounts[i] = 0;
		} else {
			m << to_send[i];
			sendcounts[i] = m.size() - size;
			size = m.size();
		}
	}
	StopTimer(SERIALIZATION_TIMER);
	
	StartTimer(TRANSFER_TIMER);
	MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK,
	            MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	
	for (int i = 0; i < np; i++) {
		sendoffset[i] = (i == 0 ? 0 : sendoffset[i - 1] + sendcounts[i - 1]);
	}
	char *sendbuf = m.get_buf(); //ibinstream will delete it
	char *recvbuf = NULL;
	
	StartTimer(TRANSFER_TIMER);
	MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount,
	             MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	
	delete[] sendcounts;
	delete[] sendoffset;
	StopTimer(COMMUNICATION_TIMER);
}

template<class T>
void slaveScatter(T &to_get) { //scatter
	StartTimer(COMMUNICATION_TIMER);
	int *sendcounts;
	int recvcount;
	int *sendoffset;
	
	StartTimer(TRANSFER_TIMER);
	MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK,
	            MPI_COMM_WORLD);
	
	char *sendbuf;
	char *recvbuf = new char[recvcount]; //obinstream will delete it
	
	MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount,
	             MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	
	StartTimer(SERIALIZATION_TIMER);
	obinstream um(recvbuf, recvcount);
	um >> to_get;
	StopTimer(SERIALIZATION_TIMER);
	StopTimer(COMMUNICATION_TIMER);
}

//================================================================
//gather
template<class T>
void masterGather(vector<T> &to_get) { //gather
	StartTimer(COMMUNICATION_TIMER);
	int sendcount = 0;
	int *recvcounts = new int[np];
	int *recvoffset = new int[np];
	
	StartTimer(TRANSFER_TIMER);
	MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK,
	           MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	
	for (int i = 0; i < np; i++) {
		recvoffset[i] = (i == 0 ? 0 : recvoffset[i - 1] + recvcounts[i - 1]);
	}
	
	char *sendbuf;
	int recv_tot = recvoffset[np - 1] + recvcounts[np - 1];
	char *recvbuf = new char[recv_tot]; //obinstream will delete it
	
	StartTimer(TRANSFER_TIMER);
	MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset,
	            MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	
	StartTimer(SERIALIZATION_TIMER);
	obinstream um(recvbuf, recv_tot);
	for (int i = 0; i < np; i++) {
		if (i == me)
			continue;
		um >> to_get[i];
	}
	StopTimer(SERIALIZATION_TIMER);
	delete[] recvcounts;
	delete[] recvoffset;
	StopTimer(COMMUNICATION_TIMER);
}

template<class T>
void slaveGather(T &to_send) { //gather
	StartTimer(COMMUNICATION_TIMER);
	int sendcount;
	int *recvcounts;
	int *recvoffset;
	
	StartTimer(SERIALIZATION_TIMER);
	ibinstream m;
	m << to_send;
	sendcount = m.size();
	StopTimer(SERIALIZATION_TIMER);
	
	StartTimer(TRANSFER_TIMER);
	MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK,
	           MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	
	char *sendbuf = m.get_buf(); //ibinstream will delete it
	char *recvbuf;
	
	StartTimer(TRANSFER_TIMER);
	MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset,
	            MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	StopTimer(COMMUNICATION_TIMER);
}

//================================================================
//bcast
template<class T>
void masterBcast(T &to_send) { //broadcast
	StartTimer(COMMUNICATION_TIMER);
	
	StartTimer(SERIALIZATION_TIMER);
	ibinstream m;
	m << to_send;
	int size = m.size();
	StopTimer(SERIALIZATION_TIMER);
	
	StartTimer(TRANSFER_TIMER);
	MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
	
	char *sendbuf = m.get_buf();
	MPI_Bcast(sendbuf, size, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	
	StopTimer(COMMUNICATION_TIMER);
}

template<class T>
void slaveBcast(T &to_get) { //broadcast
	StartTimer(COMMUNICATION_TIMER);
	
	int size;
	
	StartTimer(TRANSFER_TIMER);
	MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	
	StartTimer(TRANSFER_TIMER);
	char *recvbuf = new char[size]; //obinstream will delete it
	MPI_Bcast(recvbuf, size, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	
	StartTimer(SERIALIZATION_TIMER);
	obinstream um(recvbuf, size);
	um >> to_get;
	StopTimer(SERIALIZATION_TIMER);
	
	StopTimer(COMMUNICATION_TIMER);
}


#endif

//
// Created by admin on 2018/4/1.
//

#ifndef EXP_BUTTERFLY_H
#define EXP_BUTTERFLY_H

#include "header.h"

using namespace std;

namespace butterfly {
bool intersection(size_t src, vector<size_t> srcLout, size_t dst,
		vector<size_t> dstLin) {
	srcLout.push_back(src);
	dstLin.push_back(dst);
	sort(srcLout.begin(), srcLout.end());
	sort(dstLin.begin(), dstLin.end());
	vector<size_t> result;
	set_intersection(srcLout.begin(), srcLout.end(), dstLin.begin(),
			dstLin.end(), inserter(result, result.end()));
	return !result.empty();
}

void zjh_bfs_labeling(const vector<vector<size_t> > &edge, size_t src,
		const vector<size_t> &v2l, vector<vector<size_t> > &lin,
		vector<vector<size_t> > &lout) {
	queue<size_t> Q;
	set<size_t> visited;
	for (int i = 0; i < edge[src].size(); i++)
		if (v2l[src] < v2l[edge[src][i]])
			Q.push(edge[src][i]);
	while (!Q.empty()) {
		size_t dst = Q.front();
		Q.pop();
		if (visited.insert(dst).second) {
			if (!intersection(src, lout[src], dst, lin[dst])) {
				lin[dst].push_back(src);
				for (size_t i = 0; i < edge[dst].size(); i++)
					if (v2l[src] < v2l[edge[dst][i]])
						Q.push(edge[dst][i]);
			}
		}
	}
}

double cur_time = 0;

void zjh_init(const vector<vector<size_t> > &edges,
		const vector<vector<size_t> > &redges, const vector<size_t> &v2l,
		const vector<size_t> &l2v, vector<vector<size_t> > &lin,
		vector<vector<size_t> > &lout) {
	for (size_t l = 0; l < l2v.size(); l++) {
		zjh_bfs_labeling(edges, l2v[l], v2l, lin, lout);
		zjh_bfs_labeling(redges, l2v[l], v2l, lout, lin);
	}
}

void TOLIndexQuery(vector<vector<size_t> > &graph, vector<size_t> &levels,
		vector<vector<size_t> > &lin, vector<vector<size_t> > &lout) { //4.55s

	size_t N = graph.size(), M = 0;
	vector<vector<size_t> > &edges = graph;
	vector<size_t> &v2l = levels;

	vector<vector<size_t> > redges(N);
	lin.resize(N);
	lout.resize(N);
	vector<size_t> l2v(N);

	for (size_t vid = 0; vid < graph.size(); ++vid) {
		M += graph[vid].size();
		for (int i = 0; i < graph[vid].size(); ++i) {
			redges[graph[vid][i]].push_back(vid);
		}
	}

	for (size_t vid = 0; vid < v2l.size(); ++vid) {
		l2v[v2l[vid]] = vid;
	}

	log("The Size of this Graph is (V:%d,E:%d)", N, M);

	butterfly::zjh_init(edges, redges, v2l, l2v, lin, lout);

}

void validation(vector<vector<size_t> > &graph, vector<size_t> &v2l,
		vector<vector<size_t> > &lin, vector<vector<size_t> > &lout,
		vector<vector<size_t> > &in_label, vector<vector<size_t> > &ou_label) {
	vector<size_t> l2v(v2l.size());
	for (size_t vid = 0; vid < v2l.size(); ++vid) {
		l2v[v2l[vid]] = vid;
	}
	//convert the labelidx to ididx
	for (size_t i = 0; i < in_label.size(); ++i) {
		for (size_t j = 0; j < in_label[i].size(); ++j) {
			in_label[i][j] = l2v[in_label[i][j]];
		}
	}
	for (size_t i = 0; i < ou_label.size(); ++i) {
		for (size_t j = 0; j < ou_label[i].size(); ++j) {
			ou_label[i][j] = l2v[ou_label[i][j]];
		}
	}
	log("start validation!")
	//validation
	assert(lin.size() == in_label.size());
	assert(lout.size() == ou_label.size());

	for (size_t i = 0; i < lin.size(); ++i) {
//            assert(lin[i].size()==in_label[i].size());
		sort(lin[i].begin(), lin[i].end());
		sort(in_label[i].begin(), in_label[i].end());
		if (!(lin[i].size() == in_label[i].size())) {
			log("i:%d lin[i].size():%d in_label[i].size():%d",
					i, lin[i].size(), in_label[i].size());
			vector<size_t> &a = lin[i], &b = in_label[i];
			vector<size_t> diff;

			if (a.size() > b.size()) {
				set_difference(a.begin(), a.end(), b.begin(), b.end(),
						back_inserter(diff));
			} else {
				set_difference(b.begin(), b.end(), a.begin(), a.end(),
						back_inserter(diff));
			}
			printf("{");
			for (int i = 0; i < diff.size(); ++i) {
				printf("%d,", diff[i]);
			}
			printf("}\n");

			continue;
		}
		for (size_t j = 0; j < lin[i].size(); ++j) {
//                assert(lin[i][j]==in_label[i][j]);
			if (!(lin[i][j] == in_label[i][j])) {
				printf("lin[%d][%d]:%d==in_label[%d][%d]:%d\n", i, j, lin[i][j],
						i, j, in_label[i][j]);
			}
		}
	}

	for (size_t i = 0; i < lout.size(); ++i) {
//            assert(lout[i].size()==ou_label[i].size());
		sort(lout[i].begin(), lout[i].end());
		sort(ou_label[i].begin(), ou_label[i].end());
		if (!(lout[i].size() == ou_label[i].size())) {
			log("i:%d lout[i].size():%d ou_label[i].size():%d",
					i, lout[i].size(), ou_label[i].size());

			vector<size_t> &a = lout[i], &b = ou_label[i];
			vector<size_t> diff;

			if (a.size() > b.size()) {
				set_difference(a.begin(), a.end(), b.begin(), b.end(),
						back_inserter(diff));
			} else {
				set_difference(b.begin(), b.end(), a.begin(), a.end(),
						back_inserter(diff));
			}
			printf("{");
			for (int i = 0; i < diff.size(); ++i) {
				printf("%d,", diff[i]);
			}
			printf("}\n");

			continue;
		}
		for (size_t j = 0; j < lout[i].size(); ++j) {
			//                assert(lin[i][j]==in_label[i][j]);
			if (!(lout[i][j] == ou_label[i][j])) {
				printf("lout[%d][%d]:%d==ou_label[%d][%d]:%d\n", i, j,
						lout[i][j], i, j, ou_label[i][j]);
			}
		}
	}

	log("finish validation.");
}

} // namespace butterfly

#endif //EXP_BUTTERFLY_H

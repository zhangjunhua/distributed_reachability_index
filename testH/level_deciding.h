//
// Created by Admin on 2018/4/1.
//

#ifndef TOL_LEVEL_DECIDING_H
#define TOL_LEVEL_DECIDING_H

#include "header.h"
#include <vector>
using namespace std;

namespace level_decide {
typedef unsigned long long ull;
vector<size_t> decide_lvl(vector<vector<size_t> > &graph) {
	int n = graph.size();
	vector<size_t> rdegree(n);

	// construct rgraph
	for (size_t i = 0; i < graph.size(); ++i) {
		for (size_t j = 0; j < graph[i].size(); ++j) {
			rdegree[graph[i][j]]++;
		}
	}

	//calculate mulDegree
	vector<ull> degree_mul(n);
	for (int i = 0; i < n; i++) {
		degree_mul[i] = (ull) graph[i].size() * (ull) rdegree[i];
	}

	//construct reverse Index
	map<ull, set<int> > rindex;
	for (int i = 0; i < n; i++) {
		rindex[degree_mul[i]].insert(i);
	}

	//calculate level
	vector<size_t> levels(n);
	size_t level = 0;
	for (map<ull, set<int> >::reverse_iterator deg_mul = rindex.rbegin();
			deg_mul != rindex.rend(); ++deg_mul) {
		for (set<int>::iterator vid = deg_mul->second.begin();
				vid != deg_mul->second.end(); ++vid) {
			levels[*vid] = level;
			level++;
		}
	}
	return levels;
}
}

#endif //TOL_LEVEL_DECIDING_H

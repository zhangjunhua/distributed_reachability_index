//
// Created by 24148 on 01/01/2020.
//

#ifndef REACHABILITY_BUTTERFLY_H
#define REACHABILITY_BUTTERFLY_H
#define NODE_TYPE unsigned int

#include <queue>

void load_VBDuL_graph_parts(vector<vector<NODE_TYPE>> &graph, vector<vector<NODE_TYPE>> &rgraph,
                            vector<NODE_TYPE> &level, string basename, uint num_parts = 12) {
    basename = basename + ".VBDuL";
    for (uint i = 0; i < num_parts; i++) {
        char filename[256];
        sprintf(filename, "%s%s%d", basename.c_str(), ".part", i);
        FILE *file = fopen(filename, "rb");
        if (!file) {
            printf("open file failed:%s", filename);
        }
        fseek(file, 0L, SEEK_END);
        NODE_TYPE size = static_cast<unsigned long>(ftell(file)) / sizeof(NODE_TYPE);
        rewind(file);
        vector<NODE_TYPE> buf(size);
        fread(buf.data(), sizeof(NODE_TYPE), size, file);
        fclose(file);

        NODE_TYPE vid, degree;
        size_t off = 0;
        while (off < size) {
            vid = buf[off++];
            if (vid >= graph.size()) {
                graph.resize(vid + 1);
                rgraph.resize(vid + 1);
                level.resize(vid + 1);
            }
            level[vid] = buf[off++];

            degree = buf[off++];
            for (NODE_TYPE i = 0; i < degree; i++) {
                rgraph[vid].push_back(buf[off++]);
            }

            degree = buf[off++];
            for (NODE_TYPE i = 0; i < degree; i++) {
                graph[vid].push_back(buf[off++]);
            }
        }

    }
}

namespace butterfly {
    bool debug = false;

    inline bool intersection(vector<NODE_TYPE> &srcLout, vector<NODE_TYPE> &dstLin) {
        vector<NODE_TYPE>::iterator b1 = srcLout.begin(), e1 = srcLout.end(), b2 = dstLin.begin(), e2 = dstLin.end();
        for (; b1 != e1 && b2 != e2;) {
            if (*b1 < *b2)b1++;
            else if (*b1 > *b2)b2++;
            else return true;
        }
        return false;
    }


    void
    zjh_bfs_level_bitvisit_labeling(const vector<vector<NODE_TYPE>> &edge, NODE_TYPE src,
                                    const vector<NODE_TYPE> &v2l,
                                    vector<vector<NODE_TYPE>> &lin,
                                    vector<vector<NODE_TYPE>> &lout, vector<bool> &visitedbitmap) {
        queue<NODE_TYPE> Q;
        vector<NODE_TYPE> visited;
        NODE_TYPE srclvl = v2l[src];
        for (NODE_TYPE i = 0; i < edge[src].size(); i++) {
            if (srclvl < v2l[edge[src][i]]) {
                Q.push(edge[src][i]);
            }
        }
        while (!Q.empty()) {
            NODE_TYPE dst = Q.front();
            Q.pop();
            if (!visitedbitmap[dst]) {
                visitedbitmap[dst] = true;
                visited.push_back(dst);
                if (!intersection(lout[src], lin[dst])) {
                    lin[dst].push_back(srclvl);
                    for (NODE_TYPE i = 0; i < edge[dst].size(); i++)
                        if (srclvl < v2l[edge[dst][i]]) {
                            Q.push(edge[dst][i]);
                        }
                }
            }
        }
        for (vector<NODE_TYPE>::iterator it = visited.begin(); it != visited.end(); it++) {
            visitedbitmap[*it] = false;
        }
    }

    /**
     *
     * @param graph
     * @param rgraph
     * @param level
     * @return <lin, lout>
     */
    pair<vector<vector<NODE_TYPE >>, vector<vector<NODE_TYPE >>>
    butterfly(
            const vector<vector<NODE_TYPE>> &graph,
            const vector<vector<NODE_TYPE>> &rgraph,
            const vector<NODE_TYPE> &level) {
        pair<vector<vector<NODE_TYPE >>, vector<vector<NODE_TYPE >>> indexes;
        indexes.first.resize(graph.size()), indexes.second.resize(graph.size());
        vector<NODE_TYPE> l2v(graph.size());
        for (NODE_TYPE i = 0; i < graph.size(); i++) {
            l2v[level[i]] = i;
        }
        runtime = get_current_time();
        vector<bool> visitedbitmap(graph.size(), false);
        for (NODE_TYPE l = 0; l < graph.size(); l++) {
            zjh_bfs_level_bitvisit_labeling(graph, l2v[l], level, indexes.first, indexes.second, visitedbitmap);
            zjh_bfs_level_bitvisit_labeling(rgraph, l2v[l], level, indexes.second, indexes.first, visitedbitmap);
        }
        runtime = get_current_time() - runtime;
        return indexes;
    }
} // namespace butterfly



#endif //REACHABILITY_BUTTERFLY_H

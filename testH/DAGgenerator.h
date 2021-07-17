#ifndef EXP_DAGGENERATOR_H
#define EXP_DAGGENERATOR_H
#include <set>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
using namespace std;


namespace daggenerator {

    void outputDAG(vector<vector<size_t> > &graph) {
        printf("GRAPH:\n");
        for (size_t i = 0; i < graph.size(); ++i) {
            printf("%d->[", i);
            for (size_t j = 0; j < graph[i].size(); ++j) {
                printf("%d,", graph[i][j]);
            }
            printf("]\n");
        }
    }

    typedef struct {
        size_t id;
//        size_t tlevel;
        set<size_t> ou_neibs;
    } vertex;
    template<typename T1, typename T2>
    struct tuple2 {
        T1 e1;
        T2 e2;
    };


    /**
     * algorithm to distribute n vertexes to tl levels,
     * each level contains at least 1 vertex.
     *
     * TODO
     * is this no bias distribution?
     *
     * @param n
     * @param tl
     * @return
     */
    vector<vector<vertex> > distribute_lvls(size_t n, size_t tl) {
        //first, assign level_size for each level
        vector<size_t> level_size(tl, 1);
        for (size_t total = tl; total < n; ++total) {
            level_size[rand() % tl]++;
        }
        //second, assign vertex for each level
        vector<vector<vertex> > lvl_vertexes(tl);
        for (size_t vid = 0; vid < n; ++vid) {
            size_t tlevel = rand() % tl;
            while (level_size[tlevel] == lvl_vertexes[tlevel].size()) {
                tlevel = rand() % tl;
            }
            lvl_vertexes[tlevel].resize(lvl_vertexes[tlevel].size() + 1);
            lvl_vertexes[tlevel].back().id = vid;
        }
        return lvl_vertexes;
    }

    /**
     * this index construct to indicate the level and position in level of each vertex, for example:
     * in format (level,position):[(0,0), (0,1), (0,2), (1,0), (1,1), (2,0)]
     * @param lvl_vertexes
     * @return
     */
    vector<tuple2<size_t, size_t> > level_idx(const vector<vector<vertex> > &lvl_vertexes) {
        vector<tuple2<size_t, size_t> > index;
        for (size_t tlevel = 0; tlevel < lvl_vertexes.size(); ++tlevel) {
            for (size_t i = 0; i < lvl_vertexes[tlevel].size(); ++i) {
                index.resize(index.size() + 1);
                index.back().e1 = tlevel;
                index.back().e2 = i;
            }
        }
        return index;
    }


    /**
     * This fun generates DAG using the method introduced in paper:
     *      Tf-label: a topological-folding labeling scheme for reachability querying in a large graph
     * Overall Steps:
     *      1. create n vertexes and distribute them to tl levels
     *      2. for each vertex at level i(1<i<tl), add a edge from a vertex selected randomly from level i-1 to v
     *      3. add edges from i to (tl-1) randomly selected vertexes at level j>i in G.
     *
     * @param n number of vertexes
     * @param davg avg degree of vertexes
     * @param tl total topological levels of this dag
     * @return DAG
     */
    vector<vector<size_t> > gen(size_t n, size_t davg, size_t tl, int seed=0) {
        //step 1. create n vertex and distribute them across tl levels
        srand(seed);
        vector<vector<vertex> > lvl_vertexes = distribute_lvls(n, tl);
//        {//debug
//            for(size_t i=0;i<lvl_vertexes.size();++i){
//                printf("lvl:%d [",i);
//                for(size_t j=0;j<lvl_vertexes[i].size();++j){
//                    printf("%d,",lvl_vertexes[i][j].id);
//                }
//                printf("]\n");
//            }
//        }

        //construct index for simplicity of computation
        vector<tuple2<size_t, size_t> > index1 = level_idx(lvl_vertexes);
        vector<size_t> level_begin_idx(tl, 0);
        for (size_t tlevel = 1; tlevel < tl; ++tlevel) {
            level_begin_idx[tlevel] = level_begin_idx[tlevel - 1] + lvl_vertexes[tlevel - 1].size();
        }


        //step 2
        for (size_t tlevel = 1; tlevel < tl; ++tlevel) {//for each level
            for (size_t i = 0; i < lvl_vertexes[tlevel].size(); ++i) {//for each v in this level
                //find and add a parent vertex in level (tlevel-1)
                lvl_vertexes[tlevel - 1][rand() % lvl_vertexes[tlevel - 1].size()].ou_neibs.insert(
                        lvl_vertexes[tlevel][i].id);
            }
        }
        //step 3
        for (size_t tlevel = 0; tlevel < tl - 1; ++tlevel) {//for each level
            for (size_t i = 0; i < lvl_vertexes[tlevel].size(); ++i) {//for each v in this level
                set<size_t> &ou_neibs = lvl_vertexes[tlevel][i].ou_neibs;

                //add davg-1 fwd edges to this vertex
                for (size_t j = 0; j < davg - 1; ++j) {//add tl-1 edges from v to lvl_vertexes with lower level.
                    if (ou_neibs.size() == (n - level_begin_idx[tlevel + 1])) {
                        //the out_neighbor contains all lvl_vertexes with lower level, break;
                        break;
                    }
                    tuple2<size_t, size_t> v = index1[rand() % (n - level_begin_idx[tlevel + 1]) +
                                                      level_begin_idx[tlevel + 1]];
                    while (!ou_neibs.insert(lvl_vertexes[v.e1][v.e2].id).second) {
                        v = index1[rand() % (n - level_begin_idx[tlevel + 1]) + level_begin_idx[tlevel + 1]];
                    }
                }
            }
        }

        vector<vector<size_t> > graph(n);
        for (int i = 0; i < lvl_vertexes.size(); i++) {
            for (int j = 0; j < lvl_vertexes[i].size(); j++) {
                vertex &v = lvl_vertexes[i][j];
                graph[v.id].resize(v.ou_neibs.size());

                vector<size_t>::iterator nb1;
                set<size_t>::iterator nb2;
                for (nb1 = graph[v.id].begin(), nb2 = v.ou_neibs.begin();
                     nb1 != graph[v.id].end(), nb2 != v.ou_neibs.end(); (*nb1++) = (*nb2++));
            }
        }
        return graph;
    }

    void test() {
        vector<vector<size_t> > graph = gen(10, 2, 5);
        outputDAG(graph);
    }
}
#endif //EXP_DAGGENERATOR_H

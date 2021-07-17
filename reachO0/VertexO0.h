#ifndef VERTEX_HO0
#define VERTEX_HO0

#include "../utils/global.h"
#include <vector>
#include "../utils/serialization.h"

using namespace std;
namespace O0 {
    NODETYPE vid2idx(NODETYPE vid) {
        return vid / np;
    }

    bool set_intersection(vector<NODETYPE> &a, vector<NODETYPE> &b) {
        int size_1 = 0;
        int size_2 = 0;
        while (size_1 < a.size() && size_2 < b.size()) {
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
        Vertex() {
        }

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
            for (char i = 0; i < om.size(); i++) {
                if (om[i]) {
                    this->om.push_back(i);
                }
            }
        }

        NODETYPE id;
        NODETYPE level;
        vector<char> im, om;//in(out) neighbor machine
        vector<NODETYPE> ilb, olb;//in and out Labels

        set<NODETYPE> iVvisit, oVvisit;
        set<NODETYPE> iNvisit, oNvisit;
    };

    obinstream &operator>>(obinstream &obs, Vertex &v) {
        obs >> v.id >> v.level >> v.im >> v.om;
        return obs;
    }

    ibinstream &operator<<(ibinstream &ibs, const Vertex &v) {
        ibs << v.id << v.level << v.im << v.om;
        return ibs;
    }
}

#endif //VERTEX_HO2a

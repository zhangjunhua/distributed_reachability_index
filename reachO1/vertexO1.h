//
// Created by zjh on 9/9/19.
//

#ifndef REACHABILITY_VERTEXO1_H
#define REACHABILITY_VERTEXO1_H

#include "../utils/global.h"
#include "../utils/serialization.h"

namespace O1 {
    class message {
    public:
        NODETYPE level;
        char status;

        message() : status(0) {}

        inline void set_add() { status &= ~1; }

        inline void set_del() { status |= 1; }

        inline void set_forward() { status &= ~2; }

        inline void set_backward() { status |= 2; }

        inline bool is_del() { return status & 1; }

        inline bool is_add() { return !is_del(); }

        inline bool is_backward() { return status & 2; }

        inline bool is_forward() { return !is_backward(); }
    };

    obinstream &operator>>(obinstream &obs, message &msg) {
        obs >> msg.level >> msg.status;
        return obs;
    }

    ibinstream &operator<<(ibinstream &ibs, const message &msg) {
        ibs << msg.level << msg.status;
        return ibs;
    }

    class centralized_vertex {
    public:
        NODETYPE id, level;
        vector<NODETYPE> ineb, oneb;
    };

    class vertex {
    public:
        NODETYPE id, level;
        vector<char> inebM, onebM;

        vector<NODETYPE> ilb, olb;
        vector<NODETYPE> ilb_cand, olb_cand;
        vector<bool> fav, fdv, bav, bdv;

        vertex() {}

        vertex(centralized_vertex &v) {
            id = v.id;
            level = v.level;
            vector<bool> im(np, false), om(np, false);
            for (NODETYPE i:v.ineb)
                im[vid2workerid(i)] = true;
            for (NODETYPE i:v.oneb)
                om[vid2workerid(i)] = true;
            for (char i = 0; i < im.size(); i++) {
                if (im[i])inebM.push_back(i);
            }
            for (char i = 0; i < om.size(); i++) {
                if (om[i])onebM.push_back(i);
            }
        }

        void init(size_t num_vertex) {
            fav.resize(num_vertex, false);
            fdv.resize(num_vertex, false);
            bav.resize(num_vertex, false);
            bdv.resize(num_vertex, false);
        }
    };

    obinstream &operator>>(obinstream &obs, vertex &v) {
        obs >> v.id >> v.level >> v.inebM >> v.onebM;
        return obs;
    }

    ibinstream &operator<<(ibinstream &ibs, const vertex &v) {
        ibs << v.id << v.level << v.inebM << v.onebM;
        return ibs;
    }

}

#endif //REACHABILITY_VERTEXO1_H

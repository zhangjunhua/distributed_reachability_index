#ifndef VERTEXdelana_H
#define VERTEXdelana_H

#include "../utils/global.h"
#include <vector>
#include "../utils/serialization.h"

using namespace std;
namespace delana {
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

//    typedef KeyT KeyType;
//    typedef ValueT ValueType;
//    typedef MessageT MessageType;
//    typedef HashT HashType;
//    typedef vector<MessageType> MessageContainer;
//    typedef typename MessageContainer::iterator MessageIter;
//    typedef Vertex<KeyT, ValueT, MessageT, HashT> VertexT;
//    typedef MessageBuffer <VertexT> MessageBufT;

//    friend ibinstream &operator<<(ibinstream &m, const VertexT &v) {
//        m << v.id;
//        m << v._value;
//        return m;
//    }
//
//    friend obinstream &operator>>(obinstream &m, VertexT &v) {
//        m >> v.id;
//        m >> v._value;
//        return m;
//    }

//    virtual void compute(MessageContainer &messages) = 0;
//
//    virtual void postbatch_compute() = 0;

//    inline ValueT &value() {
//        return _value;
//    }

//    inline const ValueT &value() const {
//        return _value;
//    }

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
        vector<NODETYPE> iml, oml;//in(out) neighbor max level
        vector<NODETYPE> ilb, olb;
        map<NODETYPE, bool> ist, ost;
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

#endif //VERTEXdelana_H

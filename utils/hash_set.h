//
// Created by 24148 on 31/12/2019.
//

#ifndef REACHABILITY_HASH_SET_H
#define REACHABILITY_HASH_SET_H

#include <vector>

namespace koala {
	class my_openadd_hashset {
#define LoadfactorInverse 1.5
#define nullele (4294967295U)
	public:
		my_openadd_hashset() : hash_table(0, nullele), _size(0) {}
		
		inline bool insert(unsigned int key) {
			if ((_size + 1) * LoadfactorInverse > hash_table.size()) {
				expand();
			}
			if (put(key, hash_table)) {
				_size++;
				return true;
			} else {
				return false;
			}
		}
		
		inline bool find(unsigned int key) const {
			if (_size == 0)
				return false;
			unsigned int idx = key % hash_table.size();
			unsigned int tmpi = (idx + hash_table.size() - 1) % hash_table.size();
			while (hash_table[idx] != nullele && hash_table[idx] != key && tmpi != idx) {
				++idx;
				idx = idx == hash_table.size() ? 0 : idx;
			}
			if (hash_table[idx] == key) {
				return true;
			}
			return false;
		}
		
		inline bool intersection(const std::vector<unsigned int> &arr) const {
			for (auto a:arr) {
				if (find(a))return true;
			}
			return false;
		}
		
		inline bool intersection(const my_openadd_hashset *hs) const {
			if (hs->size() <= size()) {
				for (const auto &e:hs->hash_table) {
					if (e != nullele && find(e))return true;
				}
				return false;
			} else {
				return hs->intersection(this);
			}
		}
		
		inline void clear() {
			hash_table.clear(), hash_table.shrink_to_fit();
			_size = 0;
		}
		
		inline std::vector<unsigned int> &release_data() {
			return hash_table;
		}
		
		inline unsigned int size() const { return _size; }
	
	private:
		static inline bool put(unsigned int key, std::vector<unsigned int> &table) {
			unsigned int idx = key % table.size();
			while (table[idx] != nullele && table[idx] != key) {
				++idx;
				idx = idx == table.size() ? 0 : idx;
			}
			if (table[idx] == nullele) {
				table[idx] = key;
				return true;
			}
			return false;
		}
		
		inline void expand() {
			unsigned int _new_size = 0;
			if (hash_table.size() == 0)_new_size = 1;
			else _new_size = hash_table.size() * 2;
			std::vector<unsigned int> newtable(_new_size, nullele);
			for (auto a:hash_table)if (a != nullele)put(a, newtable);
			swap(newtable, hash_table);
		}
		
		std::vector<unsigned int> hash_table;
		unsigned int _size;
	};
}

#endif //REACHABILITY_HASH_SET_H

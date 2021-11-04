//
// Created by 24148 on 11/4/2021.
//
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <dirent.h>
#include <sys/stat.h>

using namespace std;

vector<string> getFileList(std::string path) {
  if (path.back() != '/') {// dir always end with /
    path.push_back('/');
  }
  DIR *dirp;
  struct dirent *directory;
  struct stat statebuf;
  vector<string> file_list;
  dirp = opendir(path.c_str());
  if (dirp == NULL) {
    cerr << "path " << path << " not found." << endl;
    exit(-1);
  }
  while ((directory = readdir(dirp)) != NULL) {
    if (directory->d_type == DT_REG) {//regular file
      string filepath = path + directory->d_name;
      file_list.emplace_back(filepath);
    }
  }
  closedir(dirp);
  return file_list;
}

inline bool
query(const vector<int> &srcLout, const int srcl, const vector<int> &dstLin, const int dstl) {
  vector<int>::const_iterator b1 = srcLout.begin(), e1 = srcLout.end(), b2 = dstLin.begin(), e2 = dstLin.end();
  for (; b1 != e1 && b2 != e2;) {
    if (*b1 < *b2)b1++;
    else if (*b1 > *b2)b2++;
    else return true;
  }

  if (srcl < dstl) {
    while (b2 != e2) {
      if (srcl > *b2)b2++;
      else return srcl == *b2;
    }
  } else if (srcl > dstl) {
    while (b1 != e1) {
      if (*b1 < dstl)b1++;
      else return *b1 == dstl;
    }
  } else {
    return true;
  }
  return false;
}

int main(int argc, char *argv[]) {
  string idx_dir = argv[1], q_path = argv[2];
  auto idx_files = getFileList(idx_dir);

  vector<vector<int>> Lin, Lout;
  vector<int> order;
  vector<pair<int, int>> queries;
  int n = 0, vid, ord, sz, s, t;
  for (auto fpath:idx_files) {//load index
    ifstream ifs(fpath);
    if (!ifs) {
      cout << "open file " << fpath << " failed." << endl;
      ifs.close();
      exit(-1);
    }
    while (ifs >> vid >> ord) {
      if (vid + 1 > n) {
        n = vid + 1;
        Lin.resize(n);
        Lout.resize(n);
        order.resize(n);
      }
      order[vid] = ord;

      ifs >> sz;
      Lin[vid].resize(sz);
      for (int i = 0; i < sz; ++i) {
        ifs >> Lin[vid][i];
      }

      ifs >> sz;
      Lout[vid].resize(sz);
      for (int i = 0; i < sz; ++i) {
        ifs >> Lout[vid][i];
      }
    }
    ifs.close();
  }

//  {
//    for (int i = 0; i < order.size(); ++i) {
//      cout << i + 1 << " can reach: ";
//      for (int j = 0; j < order.size(); ++j) {
//        if (query(Lout[i], order[i], Lin[j], order[j]))
//          cout << j + 1 << " ";
//      }
//      cout << endl;
//    }
//  }

  {//load queries
    ifstream ifs(q_path);
    if (!ifs) {
      cout << "open file " << q_path << " failed." << endl;
      ifs.close();
      exit(-1);
    }
    while (ifs >> s >> t) {
      queries.emplace_back(s - 1, t - 1);
    }
    ifs.close();
  }

  //processing queries
  for (const auto &q : queries) {
    if (query(Lout[q.first], order[q.first], Lin[q.second], order[q.second]))
      printf("Reach(%d,%d)=True.\n", q.first + 1, q.second + 1);
    else
      printf("Reach(%d,%d)=False.\n", q.first + 1, q.second + 1);
  }

  return 0;
}
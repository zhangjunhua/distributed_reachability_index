//
// Created by zjh on 12/8/19.
//

#ifndef REACHABILITY_GRAPHIO_H
#define REACHABILITY_GRAPHIO_H

#include <string>
#include <vector>
#include <tuple>
#include <dirent.h>
#include <iostream>
#include <sys/stat.h>
#include <algorithm>

using namespace std;


vector<pair<string, int>> getFileList(std::string path) {
    if (path.back() != '/') {// dir always end with /
        path.push_back('/');
    }
    DIR *dirp;
    struct dirent *directory;
    struct stat statebuf;
    vector<pair<string, int>> file_list;
    dirp = opendir(path.c_str());
    if (dirp == NULL) {
        cerr << "path " << path << " not found." << endl;
        exit(-1);
    }
    while ((directory = readdir(dirp)) != NULL) {
        if (directory->d_type == DT_REG) {//regular file
            string filepath = path + directory->d_name;
            int size;
            if (stat(filepath.c_str(), &statebuf) == -1) {
                cerr << "read info of file " << filepath << " failed." << endl;
            } else {
                size = statebuf.st_size / (1 << 20);
            }
            file_list.emplace_back(make_pair(filepath, size));
        }
    }
    closedir(dirp);
    return file_list;
}

vector<vector<string>> dispatch_splits(vector<pair<string, int>> file_list, uint number_of_parts) {
    //sort
    sort(file_list.begin(), file_list.end(),
         [](const pair<string, int> &a, const pair<string, int> &b) { return a.second > b.second; });
    //assign file list
    vector<vector<string>> splited_parts(number_of_parts);
    vector<int> size_of_parts(number_of_parts, 0);
    for (pair<string, int> &file:file_list) {
        // find the most light loaded node
        int minsize = size_of_parts[0];
        int minidx = 0;
        for (uint i = 1; i < number_of_parts; ++i) {
            if (size_of_parts[i] < minsize) {
                minsize = size_of_parts[i];
                minidx = i;
            }
        }
        //assign this file to the most light loaded node
        splited_parts[minidx].push_back(file.first);
        size_of_parts[minidx] += file.second;
    }
    return splited_parts;
}


// load vertex graph



// load edge graph


#endif //REACHABILITY_GRAPHIO_H

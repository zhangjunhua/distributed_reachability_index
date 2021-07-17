/*
 * dfsop.h
 *
 *  Created on: May 24, 2018
 *      Author: giraph
 */

#ifndef DFSOP_H_
#define DFSOP_H_
#include <sstream>
#include <string>
#include <vector>
#include <cstdio>
#include "../utils/ydhdfs.h"
using namespace std;

namespace dfsop {
void writeDFS(vector<vector<size_t> > &graph, vector<size_t> &levels,
		const char * odir) {
	//connect dfs
	hdfsFS fs = hdfsConnect("master", 9000);
	if (!fs) {
		fprintf(stderr, "Failed to connect to HDFS!\n");
		exit(-1);
	}

	//open a file
	char * fname = "graph";
	char* filePath = new char[strlen(odir) + strlen(fname) + 2];
	sprintf(filePath, "%s/%s", odir, fname);
	hdfsFile hdl = hdfsOpenFile(fs, filePath, O_WRONLY | O_CREAT, 0, 0, 0);
	if (!hdl) {
		fprintf(stderr, "Failed to open %s for writing!\n", filePath);
		exit(-1);
	}
	delete[] filePath;

	/**
	 * vertex input format:
	 * id\tlevel outdegree outneighbors indegree inneighbors
	 * id start from 0, and level also starts from 0
	 */
	//construct rgraph
	vector<vector<size_t> > rgraph(graph.size());
	for (size_t i = 0; i < graph.size(); ++i) {
		for (size_t j = 0; j < graph[i].size(); ++j) {
			rgraph[graph[i][j]].push_back(i);
		}
	}

	//write data
	ostringstream oss;
	for (size_t vid = 0; vid < graph.size(); ++vid) {
		oss << vid << "\t" << levels[vid] << " ";

		oss << graph[vid].size() << " ";
		for (size_t i = 0; i < graph[vid].size(); ++i) {
			oss << graph[vid][i] << " ";
		}

		oss << rgraph[vid].size() << " ";
		for (size_t i = 0; i < rgraph[vid].size(); ++i) {
			oss << rgraph[vid][i] << " ";
		}

		oss << "\n";

	}
	string s;

	tSize numWritten = hdfsWrite(fs, hdl, oss.str().c_str(), oss.str().size());
	if (numWritten == -1) {
		fprintf(stderr, "Failed to write file!\n");
		exit(-1);
	}

	//flush content and close the file
	if (hdfsFlush(fs, hdl)) {
		fprintf(stderr, "Failed to 'flush' %s\n", filePath);
		exit(-1);
	}
	hdfsCloseFile(fs, hdl);

	//disconnect the dfs
	hdfsDisconnect(fs);

}

void readDFS(vector<vector<size_t> > &in_label,
		vector<vector<size_t> > &ou_label, const char * idir) {

	hdfsFS fs = hdfsConnect("master", 9000);
	if (!fs) {
		fprintf(stderr, "Failed to connect to HDFS!\n");
		exit(-1);
	}
	/**
	 * list files in the directory
	 */
	int numFiles;
	///reachability/output/twitter_middle
	hdfsFileInfo* fileinfo = hdfsListDirectory(fs, idir, &numFiles);
	/**
	 * iterate all files and read the data
	 */
	for (int i = 0; i < numFiles; ++i) {
		hdfsFile in = getRHandle(fileinfo[i].mName, fs);
		printf("filename:%s\n", fileinfo[i].mName);
		LineReader reader(fs, in);
		while (true) {
			//read one line from the file
			reader.readLine();
			if (!reader.eof()) {
				//get and handle this line
				const char *line = reader.getLine();

				istringstream iss(line);
				size_t vid;
				string tok;

				iss >> vid;
				iss >> tok;

				in_label.resize(max(vid + 1, in_label.size()));
				ou_label.resize(in_label.size());

				//handle in label
				while (iss >> tok) {
					if (tok.at(0) >= '0' && tok.at(0) <= '9')
						in_label[vid].push_back(atoi(tok.c_str()));
					else
						break;
				}
				//handle out label
				while (iss >> tok) {
					if (tok.at(0) >= '0' && tok.at(0) <= '9')
						ou_label[vid].push_back(atoi(tok.c_str()));
				}
			} else
				break;
		}
		hdfsCloseFile(fs, in);
	}

	hdfsDisconnect(fs);

}
}

#endif /* DFSOP_H_ */

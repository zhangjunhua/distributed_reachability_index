//
// Created by zjh on 19-4-12.
//

#ifndef REACHABILITY_CMDOPTSPARSER_H
#define REACHABILITY_CMDOPTSPARSER_H

#include <string>
#include <algorithm>

char* getCmdOption(int argc, char ** argv, const std::string & option)
{
    char ** itr = std::find(argv, argv+argc, option);
    if (itr != (argv+argc) && ++itr != (argv+argc))
    {
        return *itr;
    }
    return 0;
}

bool cmdOptionExists(int argc, char ** argv, const std::string& option)
{
    return std::find(argv, argv+argc, option) != (argv+argc);
}

#endif //REACHABILITY_CMDOPTSPARSER_H

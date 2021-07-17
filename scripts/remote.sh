#!/bin/bash
cd /home/junhzhan/reach
rm a.out log.txt
cd /home/junhzhan/reach/dist_reach
make >>../log.txt 2>&1

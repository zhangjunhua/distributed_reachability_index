#!/usr/bin/bash

test_SBL_SBLp_varing_nodes() {
  #2020-10-05
  datasets="FULL_USA yahooo_advertisers flickr amazon0601 Gnutella soc-sinaweibo"
  datahome=/data/junhzhan/drlData
  nodes="32 16 8 4 2 1"
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="3"
  for data in $datasets; do
    for core_num in $nodes; do
      mpiexec -n $core_num drl \
        -if $datahome/$data/VBDuL.parts \
        -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
    done
  done
}

test_SBL_SBLp_varing_batch_setting() {
  datasets="FULL_USA yahooo_advertisers flickr amazon0601 Gnutella soc-sinaweibo"
  datahome=/data/junhzhan/drlData
  nodes="32"
  iniBses="1 2 4 8 16 32 64 128"
  maxBsize=262144
  bsize_growrate=2
  #  iniBsize=2

  opt_level="3"
  for data in $datasets; do
    for core_num in $nodes; do
      for iniBsize in $iniBses; do
        mpiexec -n $core_num drl \
          -if $datahome/$data/VBDuL.parts \
          -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
      done
    done
  done
}

test_SBL_SBLp_32_nodes() {
  #2020-10-05
  datasets="wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 sk-2005 uk-2006-05"

  datasets="webbase_2001 host_linkage webspam_uk2007 uk-2006-05"
  datasets="host_linkage"
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=1.1
  iniBsize=1

  opt_level="3a"
  core_num="32"

  for data in $datasets; do
    mpiexec -n $core_num drl2 \
      -if $datahome/$data/VBDuL.parts \
      -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
  done
}

test_SBL_SBLp_32_nodes
echo $'\a'

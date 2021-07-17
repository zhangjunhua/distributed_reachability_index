#!/usr/bin/bash

function test_SBL_SBLp_varing_nodes() {
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

function test_SBL_SBLp_varing_batch_setting() {
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

function test_SBL_SBLp_32_nodes() {
  #2020-10-05
  datasets="wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 sk-2005 uk-2006-05"

  datasets="host_linkage"
  datasets="gsh-2015-host"
datasets="graph500"
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="3"
  core_num="32"

  for data in $datasets; do
    mpiexec -n $core_num drl \
      -if $datahome/$data/VBDuL.parts \
      -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
  done
}

function test_sbl_32_nodes() {
  #2020-10-11
  datasets="wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 sk-2005 uk-2006-05"

  datasets="host_linkage"
  datasets="gsh-2015-host"
  datasets="graph500"
  datasets="yahooo_advertisers flickr pokec"
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="2a"
  core_num="32"

  for data in $datasets; do
    mpiexec -n $core_num drl \
      -if $datahome/$data/VBDuL.parts \
      -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
  done >/home/junhzhan/data/run_results/test_sbl_32_nodes_"$(date +%Y%m%d_%T)".txt
}

function test_sbl_varing_nodes() {
  #2020-10-05
  datasets="yahooo_advertisers"
  datahome=/data/junhzhan/drlData
  nodes="32 16 8 4 2 1"
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="2a"
  for data in $datasets; do
    for core_num in $nodes; do
      mpiexec -n $core_num drl \
        -if $datahome/$data/VBDuL.parts \
        -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
    done
  done>/home/junhzhan/data/run_results/test_sbl_varing_nodes_"$(date +%Y%m%d_%T)".txt
}

function test_sblp_varing_iniBses(){
  datasets="yahooo_advertisers flickr"
  datahome=/data/junhzhan/drlData
  nodes="32"
  iniBses="128 64 32 16 8 4 2 1"
  maxBsize=262144
  bsize_growrate=2

  opt_level="3"
  for data in $datasets; do
    for core_num in $nodes; do
      for iniBsize in $iniBses;do
        mpiexec -n $core_num drl \
          -if $datahome/$data/VBDuL.parts \
          -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
        done
    done
  done>/home/junhzhan/data/run_results/test_sblp_varing_iniBses_"$(date +%Y%m%d_%T)".txt
}

function test_sbl_sblp_scalability(){
  datasets="yahooo_advertisers flickr"
  datahome=/data/junhzhan/drlData
  nodes="32"
  iniBses="2"
  exts=".20 .40 .60 .80"
  maxBsize=262144
  bsize_growrate=2

  opt_levels="3 2a"
  for opt_level in $opt_levels; do
    for data in $datasets; do
      for core_num in $nodes; do
        for iniBsize in $iniBses;do
          for ext in $exts;do
            mpiexec -n $core_num drl \
              -if $datahome/$data$ext/VBDuL.parts \
              -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
          done
        done
      done
    done
  done>/home/junhzhan/data/run_results/test_sbl_sblp_scalability_"$(date +%Y%m%d_%T)".txt
}

function verify_O5_program() {
  datasets="citeseer citeseerx cit_patent cit_patent_snap dbpedia \
   go_uniprot uniprotenc_22m uniprotenc_100m uniprotenc_150m \
   email_EuAll soc_LiveJournal1 web_Google wiki_Talk govwild \
   yago twitter twitter_mpi web_uk webspam_uk2007 host_linkage friendster"
  datasets="wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 sk-2005 uk-2006-05"

  datasets="host_linkage"
  datasets="gsh-2015-host"
  datasets="graph500"
  datasets="yahooo_advertisers flickr pokec"
  datasets="citeseer"

  datasets="citeseer citeseerx cit_patent cit_patent_snap dbpedia \
   go_uniprot uniprotenc_22m uniprotenc_100m uniprotenc_150m \
   email_EuAll soc_LiveJournal1 web_Google wiki_Talk govwild \
   yago twitter twitter_mpi web_uk webspam_uk2007 host_linkage friendster"
#   datasets="email_EuAll"
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="5"
  core_num="32"
#  echo /home/junhzhan/data/run_results/drl."$FUNCNAME"_"$(date +%Y%m%d_%T)".txt
  for data in $datasets; do
    for opt_level in 4 5;do
    mpiexec -n $core_num drl \
      -if $datahome/$data/VBDuL.parts \
      -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
    done
  done >/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
  echo $'\a'
}

function debug_O5_program() {
  make
  datasets="email_EuAll"
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="5"
  core_num="32"
#  echo /home/junhzhan/data/run_results/drl."$FUNCNAME"_"$(date +%Y%m%d_%T)".txt
  for data in $datasets; do
    for opt_level in 4 5;do
    mpiexec -n $core_num drl \
      -if $datahome/$data/VBDuL.parts \
      -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
    done
  done
}

function run_O5_with_32nodes() {
  #ABL+
    make
    datasets="citeseer citeseerx cit_patent cit_patent_snap dbpedia go_uniprot uniprotenc_22m uniprotenc_100m \
    uniprotenc_150m email_EuAll soc_LiveJournal1 web_Google wiki_Talk govwild yago twitter twitter_mpi web_uk \
    webspam_uk2007 host_linkage friendster wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 \
    sk-2005 uk-2006-05 FULL_USA yahooo_advertisers flickr amazon0601 Gnutella soc-sinaweibo gsh-2015-host graph500"
    datasets="webbase_2001 wikipedia_link_en eu_2015_host web_wikipedia2009"
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="5"
  core_num="32"
#  echo /home/junhzhan/data/run_results/drl."$FUNCNAME"_"$(date +%Y%m%d_%T)".txt
  for data in $datasets; do
    for opt in $opt_level;do
    mpiexec -n $core_num drl \
      -if $datahome/$data/VBDuL.parts \
      -O $opt -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
    done
  done >/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
}

function run_O3_with_32nodes() {
  #SBL+
  make
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  datasets="citeseer citeseerx cit_patent cit_patent_snap dbpedia go_uniprot uniprotenc_22m uniprotenc_100m \
  uniprotenc_150m email_EuAll soc_LiveJournal1 web_Google wiki_Talk govwild yago twitter twitter_mpi web_uk \
  webspam_uk2007 host_linkage friendster wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 \
  sk-2005 uk-2006-05 FULL_USA yahooo_advertisers flickr amazon0601 Gnutella soc-sinaweibo gsh-2015-host graph500"
  datasets="webbase_2001 wikipedia_link_en eu_2015_host web_wikipedia2009"
  iniBsize=2

  opt_level="3"
  core_num="32"
#  echo /home/junhzhan/data/run_results/drl."$FUNCNAME"_"$(date +%Y%m%d_%T)".txt
  for data in $datasets; do
    for opt in $opt_level;do
    mpiexec -n $core_num drl \
      -if $datahome/$data/VBDuL.parts \
      -O $opt -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
    done
  done >/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
}

function run_O4m1_with_32nodes() {
  #ABL
  make
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  datasets="citeseer citeseerx cit_patent cit_patent_snap dbpedia go_uniprot uniprotenc_22m uniprotenc_100m \
  uniprotenc_150m email_EuAll soc_LiveJournal1 web_Google wiki_Talk govwild yago twitter twitter_mpi web_uk \
  webspam_uk2007 host_linkage friendster wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 \
  sk-2005 uk-2006-05 FULL_USA yahooo_advertisers flickr amazon0601 Gnutella soc-sinaweibo gsh-2015-host graph500"
  datasets="webbase_2001 wikipedia_link_en eu_2015_host web_wikipedia2009"
  iniBsize=2

  opt_level="4m1"
  core_num="32"
#  echo /home/junhzhan/data/run_results/drl."$FUNCNAME"_"$(date +%Y%m%d_%T)".txt
  for data in $datasets; do
    for opt in $opt_level;do
    mpiexec -n $core_num drl \
      -if $datahome/$data/VBDuL.parts \
      -O $opt -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
    done
  done >/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
}

function run_O2a_with_32nodes() {
  #SBL
  make
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  datasets="citeseer citeseerx cit_patent cit_patent_snap dbpedia go_uniprot uniprotenc_22m uniprotenc_100m \
  uniprotenc_150m email_EuAll soc_LiveJournal1 web_Google wiki_Talk govwild yago twitter twitter_mpi web_uk \
  webspam_uk2007 host_linkage friendster wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 \
  sk-2005 uk-2006-05 FULL_USA yahooo_advertisers flickr amazon0601 Gnutella soc-sinaweibo gsh-2015-host graph500"
  datasets="webbase_2001 wikipedia_link_en eu_2015_host web_wikipedia2009"
  iniBsize=2

  opt_level="2a"
  core_num="32"
#  echo /home/junhzhan/data/run_results/drl."$FUNCNAME"_"$(date +%Y%m%d_%T)".txt
  for data in $datasets; do
    for opt in $opt_level;do
    mpiexec -n $core_num drl \
      -if $datahome/$data/VBDuL.parts \
      -O $opt -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
    done
  done >/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
}

function run_O4_with_32nodes() {
  #ABL+
    make
    datasets="citeseer citeseerx cit_patent cit_patent_snap dbpedia go_uniprot uniprotenc_22m uniprotenc_100m \
    uniprotenc_150m email_EuAll soc_LiveJournal1 web_Google wiki_Talk govwild yago twitter twitter_mpi web_uk \
    webspam_uk2007 host_linkage friendster wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 \
    sk-2005 uk-2006-05 FULL_USA yahooo_advertisers flickr amazon0601 Gnutella soc-sinaweibo gsh-2015-host graph500"
    datasets="webbase_2001 wikipedia_link_en eu_2015_host web_wikipedia2009"

  datasets="citeseer citeseerx cit_patent cit_patent_snap dbpedia go_uniprot uniprotenc_22m uniprotenc_100m \
    uniprotenc_150m email_EuAll soc_LiveJournal1 web_Google wiki_Talk govwild yago twitter twitter_mpi web_uk \
    webspam_uk2007 host_linkage friendster wikivote socEpinions pokec googleplus arabic-2005 twitter-2010 it-2004 \
    sk-2005 uk-2006-05 FULL_USA yahooo_advertisers flickr amazon0601 Gnutella soc-sinaweibo gsh-2015-host graph500 \
    webbase_2001 wikipedia_link_en eu_2015_host web_wikipedia2009"
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="4"
  core_num="32"
#  echo /home/junhzhan/data/run_results/drl."$FUNCNAME"_"$(date +%Y%m%d_%T)".txt
  for data in $datasets; do
    for opt in $opt_level;do
    mpiexec -n $core_num drl \
      -if $datahome/$data/VBDuL.parts \
      -O $opt -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
    done
  done >/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
}

function run_with_varing_nodes() {
  #Friday, 22 January 2021 (AEDT)
  make

  datasets="citeseer web_wikipedia2009 dbpedia citeseerx cit_patent twitter govwild go_uniprot \
    wikipedia_link_en graph500 twitter-2010"
  datahome=/data/junhzhan/drlData
  iniBses="2"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="4 4m1 2a"
  nodes="32 16 8 4 2 1"
#  core_num="32"
#  echo /home/junhzhan/data/run_results/drl."$FUNCNAME"_"$(date +%Y%m%d_%T)".txt
  for data in $datasets; do
    for opt in $opt_level;do
      for core_num in $nodes;do
        mpiexec -n $core_num drl \
          -if $datahome/$data/VBDuL.parts \
          -O $opt -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
#        echo $data $opt $core_num
      done
    done
  done >/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
}

function run_with_dataset_size() {
  #Friday, 22 January 2021 (AEDT)
  make

  datasets="citeseer web_wikipedia2009 dbpedia citeseerx cit_patent twitter govwild go_uniprot \
    wikipedia_link_en graph500 twitter-2010"
  datahome=/data/junhzhan/drlData
  iniBses="2"
  exts=".20 .40 .60 .80"
  maxBsize=262144
  bsize_growrate=2
  iniBsize=2

  opt_level="4 4m1 2a"
  nodes="32"
#  core_num="32"
#  echo /home/junhzhan/data/run_results/drl."$FUNCNAME"_"$(date +%Y%m%d_%T)".txt
  for data in $datasets; do
    for opt in $opt_level;do
      for core_num in $nodes;do
        for ext in $exts;do
          mpiexec -n $core_num drl \
            -if $datahome/$data$ext/VBDuL.parts \
            -O $opt -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
  #        echo $data $opt $core_num
        done
      done
    done
  done >/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
}

function run_with_varing_iniBsize() {
  #7:15 pm Thursday, 28 January 2021 (AEDT)
  datasets="web_wikipedia2009 dbpedia cit_patent twitter go_uniprot wikipedia_link_en"
  datasets="citeseerx"
  datahome=/data/junhzhan/drlData
  nodes="32"
  iniBses="128 64 32 16 8 4 2 1"
  maxBsize=262144
  bsize_growrate=2

  opt_level="4"
  for data in $datasets; do
    for core_num in $nodes; do
      for iniBsize in $iniBses;do
        mpiexec -n $core_num drl \
          -if $datahome/$data/VBDuL.parts \
          -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
        done
    done
  done>/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
}

function run_with_varing_bsize_growrate() {
  #7:15 pm Thursday, 28 January 2021 (AEDT)
  datasets="web_wikipedia2009 dbpedia cit_patent twitter go_uniprot wikipedia_link_en"
  datasets="citeseerx"
  datahome=/data/junhzhan/drlData
  nodes="32"
  iniBses="2"
  maxBsize=262144
  bsize_growrates="1 1.5 2 2.5 3 3.5 4"

  opt_level="4"
  for data in $datasets; do
    for core_num in $nodes; do
      for iniBsize in $iniBses;do
        for bsize_growrate in $bsize_growrates;do
          mpiexec -n $core_num drl \
            -if $datahome/$data/VBDuL.parts \
            -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
#            echo $core_num drl \
#            -if $datahome/$data/VBDuL.parts \
#            -O $opt_level -maxBsize $maxBsize -iniBsize $iniBsize -bsize_growrate $bsize_growrate
          done
        done
    done
  done  >/home/junhzhan/data/run_results/"$(date +%Y-%m-%d_%T)"_drl."$FUNCNAME"_.txt
  notify
}
run_with_varing_iniBsize
run_with_varing_bsize_growrate
#
#run_with_varing_node
#run_with_varing_bsize_growrate
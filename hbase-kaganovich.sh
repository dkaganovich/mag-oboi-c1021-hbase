#!/usr/bin/env bash

# set -xe -o pipefail

if [ ! -f "graph1.tsv" ]; then
    echo "Missing file: graph1.tsv"
    exit 1
fi
if [ ! -f "graph2.tsv" ]; then
    echo "Missing file: graph2.tsv"
    exit 1
fi

log_file_name='hbase-kaganovich.log'
graph1_hdfs_file_name='kaganovich-graph-1.tsv'
graph1_hfiles_dir='kaganovich-graph-1-hfiles'
graph2_hdfs_file_name='kaganovich-graph-2.tsv'
graph2_hfiles_dir='kaganovich-graph-2-hfiles'

# create dir on hdfs if not exists
echo "Preparing file system..." | tee -a "$log_file_name"
sudo -u hdfs hadoop fs -mkdir -p /user/cse6242/ 2>&1 | tee -a "$log_file_name"
sudo -u hdfs hadoop fs -chown -R cloudera:cloudera /user/cse6242/ 2>&1 | tee -a "$log_file_name"

# create hbase tables
echo "Creating tables..." | tee -a "$log_file_name"
echo "create 'kaganovich-graph-1', 'al'; create 'kaganovich-graph-2', 'al'" | hbase shell 2>&1 | tee -a "$log_file_name"

function bulkimport() {
    local input_file_name="$1"
    local hdfs_file_name="$2"
    local hfiles_dir="$3"
    local hbase_tbl_name="$4"

    # move .tsv to hdfs
    hadoop fs -put "$input_file_name" "/user/cse6242/$hdfs_file_name"

    # generate hfiles
    hadoop jar hbase-bulkimport/target/hbase-bulkimport-1.0-SNAPSHOT-job.jar edu.gatech.cse6242.Driver "/user/cse6242/$hdfs_file_name" "$hfiles_dir" "$hbase_tbl_name"

    # bulk import hfiles to hbase 
    sudo -u hdfs hadoop fs -chown -R hbase:hbase "/user/cloudera/$hfiles_dir"
    hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles "$hfiles_dir" "$hbase_tbl_name"

    # cleanup
    hadoop fs -rm -r "/user/cloudera/$hfiles_dir"
    hadoop fs -rm "/user/cse6242/$hdfs_file_name"

    return 0
}

##############
# graph1.tsv #
##############

echo "Graph1: importing..." | tee -a "$log_file_name"
bulkimport graph1.tsv "$graph1_hdfs_file_name" "$graph1_hfiles_dir" kaganovich-graph-1 2>&1 | tee -a "$log_file_name"
echo "Graph1: fetching stats..." | tee -a "$log_file_name"
cnt=$(echo "count 'kaganovich-graph-1'" | hbase shell 2>>"$log_file_name" | tee -a "$log_file_name" | sed -n '$ p')
echo "Graph1: $cnt records were created" | tee -a "$log_file_name"

##############
# graph2.tsv #
##############

echo "Graph2: importing..." | tee -a "$log_file_name"
bulkimport graph2.tsv "$graph2_hdfs_file_name" "$graph2_hfiles_dir" kaganovich-graph-2 2>&1 | tee -a "$log_file_name"
echo "Graph2: fetching stats..." | tee -a "$log_file_name"
cnt=$(echo "count 'kaganovich-graph-2'" | hbase shell 2>>"$log_file_name" | tee -a "$log_file_name" | sed -n '$ p')
echo "Graph2: $cnt records were created" | tee -a "$log_file_name"

exit 0

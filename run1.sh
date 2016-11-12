spark-submit --class edu.gatech.cse6242.Task2 --master local \
	task2-hbase/target/task2-hbase-1.0-SNAPSHOT.jar kaganovich-graph-1 /user/cse6242/task2output1-hbase

hadoop fs -getmerge /user/cse6242/task2output1-hbase task2output1-hbase.tsv
hadoop fs -rm -r /user/cse6242/task2output1-hbase

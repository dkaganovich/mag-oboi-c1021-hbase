{ time spark-submit --class edu.gatech.cse6242.Task2 --master local \
	task2-hbase/target/task2-hbase-1.0-SNAPSHOT.jar kaganovich-graph-2 /user/cse6242/task2output2-hbase; } >>"run2-time.log" 2>&1

hadoop fs -getmerge /user/cse6242/task2output2-hbase task2output2-hbase.tsv
hadoop fs -rm -r /user/cse6242/task2output2-hbase

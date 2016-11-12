package edu.gatech.cse6242

import java.nio.ByteBuffer

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.JavaConversions._

object Task2 {
  def main(args: Array[String]) {
    val sparkCtx = new SparkContext(new SparkConf().setAppName("task2-hbase"))

    val tableName = args(0)

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hBaseRDD = sparkCtx.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val node2Weight = hBaseRDD.map(tuple => tuple._2).map(result => {
      val node = ByteBuffer.wrap(result.getRow).getInt
      var totalWeight = 0
      for (cell: Cell <- result.listCells()) {
        totalWeight += ByteBuffer.wrap(cell.getValueArray, cell.getValueOffset, cell.getValueLength).getInt
      }
      (node, totalWeight)
    })

//    val node2Weight = hBaseRDD.map(tuple => tuple._2).flatMap(result => {
//      val node = ByteBuffer.wrap(result.getRow).getInt
//      val kvSeq = for {
//        cell: Cell <- result.listCells()
//      } yield (node, ByteBuffer.wrap(cell.getValueArray, cell.getValueOffset, cell.getValueLength).getInt)
//      kvSeq
//    }).reduceByKey(_ + _)

    node2Weight.map { case (node, totalWeight) => Array(node, totalWeight).mkString("\t") }
      .saveAsTextFile("hdfs://localhost:8020" + args(1))
  }
}

package com.lapyap.kafepark

/**
 * @author abbhatnagar
 */

import java.util.HashMap
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ ProducerConfig, KafkaProducer, ProducerRecord }
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel
import java.util.Calendar

object Kafka2Spark {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage:bin/spark-submit --class com.lapyap.kafepark.Kafka2Spark --master local[2]  your_machine:2181 Group_name table_name Threads")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args

    implicit val sparkConf = new SparkConf().setAppName("Kafka to Spark").set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(100))

    ssc.checkpoint("checkpoint")

    val listen = new Job_Listener(ssc)
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val table_zero = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    val windowed_table_zero = table_zero.window(new Duration(100000), new Duration(100000))

    import org.apache.spark.sql._
    import com.databricks.spark.avro._
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    val query = s"Select * from "+topics
    
    windowed_table_zero.foreachRDD { rdd =>
      {
        if (rdd.count > 0) {
          print(rdd)
          print("\n")
          val avro_rdd = sqlContext.jsonRDD(rdd)
          avro_rdd.registerTempTable(topics)
          avro_rdd.printSchema()
          
          val result = sqlContext.sql(query)
          result.collect()
          
          val today = Calendar.getInstance().getTime()
          result.saveAsAvroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/"+topics+"/"+today)
          
          //result.save("/Users/abbhatnagar/kafepark/src/main/scala/data/"+topics+"/"+today, SaveMode.Append)
          
          val publishid = result.map(row => row.getStruct(0)).collect().foreach(println)
          }
      }
    }
    
    ssc.addStreamingListener(listen)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)

  }

}
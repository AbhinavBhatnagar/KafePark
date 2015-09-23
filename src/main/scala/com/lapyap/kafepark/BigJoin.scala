package com.lapyap.kafepark

import java.util.HashMap
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ ProducerConfig, KafkaProducer, ProducerRecord }
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel
import org.slf4j.impl.StaticLoggerBinder

/**
 * @author abbhatnagar
 */

object BigJoin {
  def main(args: Array[String]) {
    implicit val sparkConf = new SparkConf().setAppName("BigJoin").set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val sc = new SparkContext()

    import org.apache.spark.sql._

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    import sqlContext.implicits._

    import com.databricks.spark.avro._

    val first = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/table1")
    val second = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/table2")
    val third = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/table3")
    val forth = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/table4")

    first.registerTempTable("SD_SYNCPUBLISH")
    second.registerTempTable("SD_SYNCPUBLISHJOB")
    third.registerTempTable("SD_SYNCSUBSCRIBE")
    forth.registerTempTable("SD_SYNCSUBSCRIBEJOB")

    val KPI_1 = """Your SQL Query"""
    val result_1 = hiveContext.sql(KPI_1)
    result_1.registerTempTable("temp_KPI_1")
    result_1.collect()
    result_1.show()
    result_1.saveAsTable("RESULT_KPI_1", SaveMode.Append)


 
    val mydata = hiveContext.table("RESULT_KPI_1")
    mydata.show()

  }
}




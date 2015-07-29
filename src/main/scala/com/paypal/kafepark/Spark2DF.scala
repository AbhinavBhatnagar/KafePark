package com.paypal.kafepark

import java.util.HashMap
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ ProducerConfig, KafkaProducer, ProducerRecord }
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.SparkConf

/**
 * @author abbhatnagar
 */
object Spark2DF {
  def Read_table()
  {
  val sparkConf = new SparkConf().setAppName("Kafka to Spark")
  implicit val sc = new SparkContext(sparkConf)
  implicit val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._
  import com.databricks.spark.avro._
  val df = sqlContext.avroFile("sample", 1)
  sc.stop()
}
}
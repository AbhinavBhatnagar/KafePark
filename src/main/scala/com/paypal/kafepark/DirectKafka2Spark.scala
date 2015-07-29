package com.paypal.kafepark

import java.util.HashMap
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
 * @author abbhatnagar
 */
object DirectKafka2Spark {
    def main(args: Array[String]) {
      if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }


    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("DirectKafka2Spark")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    lines.print()
    ssc.start()
    ssc.awaitTermination()
}
}
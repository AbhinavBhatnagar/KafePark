package com.paypal.kafepark


import java.util.HashMap
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
/**
 * @author abbhatnagar
 */
 class Job_Listener(ssc: StreamingContext) extends StreamingListener 
{
  override def onBatchCompleted(batchCompleted : StreamingListenerBatchCompleted) = synchronized 
  {
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
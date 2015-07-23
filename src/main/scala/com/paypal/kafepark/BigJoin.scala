package com.paypal.kafepark

import java.util.HashMap
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ ProducerConfig, KafkaProducer, ProducerRecord }
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel

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

    val first = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/syncpublish/")
    val second = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/syncpublishjob/")
    val third = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/syncsubscribe/")
    val forth = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/syncsubscribejob/")

    first.registerTempTable("SD_SYNCPUBLISH")
    second.registerTempTable("SD_SYNCPUBLISHJOB")
    third.registerTempTable("SD_SYNCSUBSCRIBE")
    forth.registerTempTable("SD_SYNCSUBSCRIBEJOB")

    val KPI_1 = """SELECT
                                P.OBJECTDESC,
                                P.SYSTEMCD AS SOURCESYSTEMCD,
                                S.TARGETSYSTEMCD,
                                P.HOLDFLAG AS SOURCEHOLDFLAG,
                                S.HOLDFLAG AS TARGETHOLDFLAG,
                                P.PUBLISHID,
                                S.SUBSCRIBEID,
                                to_date(from_unixtime(unix_timestamp()))  AS REPORTDATE,
                                from_unixtime(unix_timestamp()) AS CRE_TS,
                                NULL AS UPD_TS 
                         FROM SD_SYNCPUBLISH P, SD_SYNCSUBSCRIBE S
                         WHERE P.PUBLISHID = S.PUBLISHID
                                AND (P.HOLDFLAG = '0' OR P.HOLDFLAG = '6' OR P.HOLDFLAG = '24' OR P.HOLDFLAG = '20' OR P.HOLDFLAG = '11')
                                AND (S.HOLDFLAG = '0' OR S.HOLDFLAG = '6' OR S.HOLDFLAG = '24' OR S.HOLDFLAG = '20' OR S.HOLDFLAG = '11')"""

    val KPI_2 = """SELECT 
                                PJ.OBJECTDESC,
                                PJ.PUBLISHID,
                                PJ.PUBLISHJOBID,
                                PJ.SYSTEMCD AS SOURCESYSTEMCD,
                                PJ.STARTDELTADESCVAL,
                                PJ.ENDDELTADESCVAL,
                                PJ.IGNOREFLAG,
                                PJ.DELTATYPECD,
                                PJ.DELTA_SIZE AS SOURCEDELTASIZE,
                                P.UPDATESTRATEGY,
                                PJ.INSERTTS,
                                PJ.UPDATETS,
                                to_date(from_unixtime(unix_timestamp()))  AS REPORTDATE,
                                from_unixtime(unix_timestamp()) AS CRE_TS,
                                NULL AS UPD_TS
                         FROM SD_SYNCPUBLISH P, SD_SYNCPUBLISHJOB PJ 
                         WHERE P.PUBLISHID = PJ.PUBLISHID
                                AND (P.HOLDFLAG = '0' OR P.HOLDFLAG = '6' OR P.HOLDFLAG = '24' OR P.HOLDFLAG = '20' OR P.HOLDFLAG = '11')
                                AND (PJ.IGNOREFLAG = '0' OR PJ.IGNOREFLAG = '8')
                                AND PJ.PUBLISHFINALSTATUSCD = 'DONE'
                                AND (PJ.DELTATYPECD = P.DELTATYPECD OR UPDATESTRATEGY = 'FL')"""

    val KPI_3 = """SELECT
                                SJ.TARGETOBJECTDESC,
                                SJ.PUBLISHID,
                                SJ.PUBLISHJOBID,
                                SJ.SUBSCRIBEJOBID,
                                SJ.SOURCESYSTEMCD,
                                SJ.TARGETSYSTEMCD,
                                PJ.STARTDELTADESCVAL,
                                PJ.ENDDELTADESCVAL,
                                SJ.IGNOREFLAG,
                                SJ.DELTATYPECD,
                                PJ.DELTA_SIZE,
                                P.UPDATESTRATEGY,
                                SJ.INSERTTS,
                                SJ.UPDATETS,
                                to_date(from_unixtime(unix_timestamp()))  AS REPORTDATE,
                                from_unixtime(unix_timestamp()) AS CRE_TS,
                                NULL AS UPD_TS
                           FROM SD_SYNCPUBLISH P, SD_SYNCPUBLISHJOB PJ, SD_SYNCSUBSCRIBE S, SD_SYNCSUBSCRIBEJOB SJ
                           WHERE P.PUBLISHID = PJ.PUBLISHID
                                AND (P.HOLDFLAG = '0' OR P.HOLDFLAG = '6' OR P.HOLDFLAG = '24' OR P.HOLDFLAG = '20' OR P.HOLDFLAG = '11')
                                AND (PJ.IGNOREFLAG = '0' OR PJ.IGNOREFLAG = '8') AND PJ.PUBLISHFINALSTATUSCD = 'DONE'
                                AND PJ.PUBLISHID = S.PUBLISHID
                                AND (S.HOLDFLAG = '0' OR S.HOLDFLAG = '6' OR S.HOLDFLAG = '24' OR S.HOLDFLAG = '20' OR S.HOLDFLAG = '11')
                                AND S.SUBSCRIBEID = SJ.SUBSCRIBEID
                                AND S.PUBLISHID = SJ.PUBLISHID
                                AND PJ.PUBLISHJOBID = SJ.PUBLISHJOBID
                                AND (PJ.DELTATYPECD = P.DELTATYPECD OR UPDATESTRATEGY = 'FL')"""

        val KPI_4 ="""SELECT
                                PJ.OBJECTDESC,
                                SJ.PUBLISHID,
                                SJ.PUBLISHJOBID,
                                SJ.SUBSCRIBEJOBID,
                                SJ.SOURCESYSTEMCD,
                                SJ.TARGETSYSTEMCD,
                                P.HOLDFLAG AS SOURCEHOLDFLAG,
                                S.HOLDFLAG AS TARGETHOLDFLAG,
                                PJ.DELTATYPECD,
                                P.UPDATESTRATEGY,
                                PJ.BUILDDELTASTARTTS,
                                PJ.BUILDDELTASTATUSTS,
                                (cast(BUILDDELTASTATUSTS.string as timestamp) - cast(BUILDDELTASTARTTS.string as timestamp)) AS BUILDDELTA_TIME,
                                PJ.EXTRACTDATASTARTTS,
                                PJ.EXTRACTDATASTATUSTS,
                                (cast(EXTRACTDATASTATUSTS.string as timestamp) - cast(EXTRACTDATASTARTTS.string as timestamp)) AS EXTRACT_TIME,
                                SJ.LOADDATASTARTTS,
                                SJ.LOADDATASTATUSTS,
                                (cast(LOADDATASTATUSTS.string as timestamp) - cast(LOADDATASTARTTS.string as timestamp)) AS LOAD_TIME,
                                COALESCE(CAST(SRCLASTREFRESHTS as timestamp), CAST(ENDDELTADESCVAL.string AS timestamp)) AS SRCREFRESHTS,                                
                                SJ.APPLYSTARTTS,
                                SJ.APPLYSTATUSTS,
                                (cast(APPLYSTATUSTS.string as timestamp) - cast(APPLYSTARTTS.string as timestamp)) AS APPLY_TIME,
                                SJ.SUBSCRIBEFINALSTATUSTS,
                                (cast(SUBSCRIBEFINALSTATUSTS.string as timestamp) - cast(BUILDDELTASTARTTS.string as timestamp)) AS TOTAL_JOB_TIME,     
                                PJ.STARTDELTADESCVAL,
                                PJ.ENDDELTADESCVAL,
                                PJ.INSERTTS,
                                PJ.UPDATETS,
                                to_date(from_unixtime(unix_timestamp()))  AS REPORTDATE,
                                from_unixtime(unix_timestamp()) AS CRE_TS,
                                NULL AS UPD_TS
                      FROM  SD_SYNCPUBLISH P,
                            SD_SYNCSUBSCRIBE S,
                            SD_SYNCPUBLISHJOB PJ,
                            SD_SYNCSUBSCRIBEJOB SJ
                      WHERE   P.PUBLISHID = S.PUBLISHID
                                  AND P.HOLDFLAG = '0'
                                  AND S.HOLDFLAG = '0'
                                  AND P.PUBLISHID = PJ.PUBLISHID
                                  AND (PJ.IGNOREFLAG = '0' OR PJ.IGNOREFLAG = '8') AND PJ.PUBLISHFINALSTATUSCD = 'DONE'
                                  AND S.SUBSCRIBEID = SJ.SUBSCRIBEID
                                  AND S.PUBLISHID = SJ.PUBLISHID
                                  AND PJ.PUBLISHJOBID = SJ.PUBLISHJOBID
                                  AND (SJ.IGNOREFLAG = '0' OR SJ.IGNOREFLAG = '8') AND SJ.SUBSCRIBEFINALSTATUSCD = 'DONE'
                                  AND (PJ.DELTATYPECD = P.DELTATYPECD OR UPDATESTRATEGY = 'FL')
                                  AND APPLYSTARTTS.string >  '2000-01-01 00:00:00'"""
        
     val KPI_4_2 =""" SELECT
                             OBJECTDESC,
                             PUBLISHID,
                             PUBLISHJOBID,
                             SUBSCRIBEJOBID,
                             SOURCESYSTEMCD,
                             TARGETSYSTEMCD,
                             SOURCEHOLDFLAG,
                             TARGETHOLDFLAG,
                             DELTATYPECD,
                             UPDATESTRATEGY,
                             BUILDDELTASTARTTS,
                             BUILDDELTASTATUSTS,
                             BUILDDELTA_TIME,
                             EXTRACTDATASTARTTS,
                             EXTRACTDATASTATUSTS,
                             EXTRACT_TIME,
                             LOADDATASTARTTS,
                             LOADDATASTATUSTS,
                             LOAD_TIME,
                             SRCREFRESHTS,                                
                             APPLYSTARTTS,
                             APPLYSTATUSTS,
                             APPLY_TIME,
                             SUBSCRIBEFINALSTATUSTS,
                             TOTAL_JOB_TIME,     
                             STARTDELTADESCVAL,
                             ENDDELTADESCVAL,
                             CASE WHEN year(SRCREFRESHTS) < year(from_unixtime(unix_timestamp()))-1 THEN NULL
                                  ELSE (cast(SUBSCRIBEFINALSTATUSTS.string AS timestamp) - cast(SRCREFRESHTS as timestamp)) END AS TOT_REPLICATION_TIME,
                             INSERTTS,
                             UPDATETS,
                             REPORTDATE,
                             CRE_TS,
                             UPD_TS
                       FROM SD_FACT_REPLICATION_LATENCY_v01"""
                             
     val KPI_4_3 = """ SELECT
                             OBJECTDESC,
                             PUBLISHID,
                             PUBLISHJOBID,
                             SUBSCRIBEJOBID,
                             SOURCESYSTEMCD,
                             TARGETSYSTEMCD,
                             SOURCEHOLDFLAG,
                             TARGETHOLDFLAG,
                             DELTATYPECD,
                             UPDATESTRATEGY,
                             BUILDDELTASTARTTS,
                             BUILDDELTASTATUSTS,
                             BUILDDELTA_TIME,
                             EXTRACTDATASTARTTS,
                             EXTRACTDATASTATUSTS,
                             EXTRACT_TIME,
                             LOADDATASTARTTS,
                             LOADDATASTATUSTS,
                             LOAD_TIME,
                             SRCREFRESHTS,                                
                             APPLYSTARTTS,
                             APPLYSTATUSTS,
                             APPLY_TIME,
                             SUBSCRIBEFINALSTATUSTS,
                             TOTAL_JOB_TIME,     
                             STARTDELTADESCVAL,
                             ENDDELTADESCVAL,
                             TOT_REPLICATION_TIME,
                             (day(TOT_REPLICATION_TIME) * 24 * 60 * 60) +(hour(TOT_REPLICATION_TIME) * 60 * 60) + (minute(TOT_REPLICATION_TIME) *60) +  second(TOT_REPLICATION_TIME)  AS TOTAL_REPLICATION_SECS,
                             INSERTTS,
                             UPDATETS,
                             REPORTDATE,
                             CRE_TS,
                             UPD_TS
                       FROM SD_FACT_REPLICATION_LATENCY_v02"""
        
     //   AND TO_CHAR(PJ.INSERTTS, 'YYYY-MM-DD') BETWEEN CAST('$startdate' AS DATE FORMAT 'YYYY-MM-DD')
    //   AND CAST('$enddate' AS DATE FORMAT 'YYYY-MM-DD')
                               
    //
    //    val table_one = sqlContext.read.parquet("/Users/abbhatnagar/kafepark/src/main/scala/data/syncpublish/")
    //    val table_two = sqlContext.read.parquet("/Users/abbhatnagar/kafepark/src/main/scala/data/syncpublishjob/")
    ////    val table_three = sqlContext.read.load("/Users/abbhatnagar/kafepark/src/main/scala/data/syncsubscribe/")
    ////    val table_four = sqlContext.read.load("/Users/abbhatnagar/kafepark/src/main/scala/data/syncsubscribejob/")
    //
  
    //    insight_1.save("/Users/abbhatnagar/kafepark/src/main/scala/data/join1/", SaveMode.Append)
    ////    insight_2.save("/Users/abbhatnagar/kafepark/src/main/scala/data/join2/", SaveMode.Append)
    ////    insight_3.save("/Users/abbhatnagar/kafepark/src/main/scala/data/join3/", SaveMode.Append)
    ////    insight_4.save("/Users/abbhatnagar/kafepark/src/main/scala/data/join4/", SaveMode.Append)

    //    //results.saveAsTable("/Users/abbhatnagar/kafepark/src/main/scala/data/join/joined1")
    //
    //    //val answer = results.map(row => row.getStruct(0)).collect().foreach(println)
    //    //results.saveAsAvroFile("/Users/abbhatnagar/kafepark/src/main/scala/data/join/")

//    val result_1 = hiveContext.sql(KPI_1)
//    result_1.registerTempTable("SD_FACT_OBJECTS_ENABLED_v01")
//    result_1.collect()
//
//    val result_2 = hiveContext.sql(KPI_2)
//    result_2.registerTempTable("SD_FACT_SOURCE_DELTASIZE_v01")
//    result_2.collect()
//    
//    val result_3 = hiveContext.sql(KPI_3)
//    result_3.registerTempTable("SD_FACT_TARGET_DELTASIZE_v01")
//    result_3.collect()
    
    val result_4_1 = hiveContext.sql(KPI_4)
    result_4_1.registerTempTable("SD_FACT_REPLICATION_LATENCY_v01")
    result_4_1.collect()
    
    result_4_1.select("PUBLISHID").show()
    
    val result_4_2 = hiveContext.sql(KPI_4_2)
    result_4_2.registerTempTable("SD_FACT_REPLICATION_LATENCY_v02")
    result_4_2.collect()
    
    val result_4 = hiveContext.sql(KPI_4_3)
    result_4.registerTempTable("SD_FACT_REPLICATION_LATENCY_v03")
    result_4.collect()
    
    //result_4.map { t => "SRCREFRESHTS"+ t(0) }.collect().foreach(println)

    
    //data.saveAsTable("SD_FACT_OBJECTS_ENABLED_v01")
    // val sole = hiveContext.sql("select SOURCESYSTEMCD from SD_FACT_OBJECTS_ENABLED_v01")
    //val disp = sole.map(row => row.getStruct(0)).collect().foreach(println)

  }
}


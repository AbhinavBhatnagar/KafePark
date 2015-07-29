package com.paypal.kafepark

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

    val first = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/syncpublish/ABCD/")
    val second = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/syncpublishjob/ABCD/")
    val third = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/syncsubscribe/ABCD/")
    val forth = hiveContext.avroFile("/Users/abbhatnagar/kafepark/src/main/scala/data1/syncsubscribejob/ABCD/")

    first.registerTempTable("SD_SYNCPUBLISH")
    second.registerTempTable("SD_SYNCPUBLISHJOB")
    third.registerTempTable("SD_SYNCSUBSCRIBE")
    forth.registerTempTable("SD_SYNCSUBSCRIBEJOB")

    val KPI_1 = """SELECT
                         P.OBJECTDESC.string AS OBJECTDESC,
                         P.SYSTEMCD.string AS SOURCESYSTEMCD,
                         S.TARGETSYSTEMCD.string AS TARGETSYSTEMCD,
                         P.HOLDFLAG.string AS SOURCEHOLDFLAG,
                         S.HOLDFLAG.string AS TARGETHOLDFLAG,
                         P.PUBLISHID.string AS PUBLISHID,
                         S.SUBSCRIBEID.string AS SUBSCRIBEID,
                         to_date(from_unixtime(unix_timestamp()))  AS REPORTDATE,
                         from_unixtime(unix_timestamp()) AS CRE_TS,
                         cast(NULL as timestamp) AS UPD_TS 
                   FROM SD_SYNCPUBLISH P, SD_SYNCSUBSCRIBE S
                   WHERE P.PUBLISHID = S.PUBLISHID
                                AND (P.HOLDFLAG.string = '0' OR P.HOLDFLAG.string = '6' OR P.HOLDFLAG.string = '24' OR P.HOLDFLAG.string = '20' OR P.HOLDFLAG.string = '11')
                                AND (S.HOLDFLAG.string = '0' OR S.HOLDFLAG.string = '6' OR S.HOLDFLAG.string = '24' OR S.HOLDFLAG.string = '20' OR S.HOLDFLAG.string = '11')"""

    val KPI_2 = """SELECT 
                         PJ.OBJECTDESC.string AS OBJECTDESC,
                         PJ.PUBLISHID.string AS PUBLISHID,
                         PJ.PUBLISHJOBID.string AS PUBLISHJOBID,
                         PJ.SYSTEMCD.string AS SOURCESYSTEMCD,
                         PJ.STARTDELTADESCVAL.string AS STARTDELTADESCVAL,
                         PJ.ENDDELTADESCVAL.string AS ENDDELTADESCVAL,
                         PJ.IGNOREFLAG.string AS IGNOREFLAG,
                         PJ.DELTATYPECD.string AS DELTATYPECD,
                         PJ.DELTA_SIZE AS SOURCEDELTASIZE,
                         P.UPDATESTRATEGY.string AS UPDATESTRATEGY,
                         PJ.INSERTTS.string AS INSERTTS,
                         PJ.UPDATETS.string AS UPDATETS,
                         to_date(from_unixtime(unix_timestamp()))  AS REPORTDATE,
                         from_unixtime(unix_timestamp()) AS CRE_TS,
                         cast(NULL as timestamp) AS UPD_TS
                   FROM SD_SYNCPUBLISH P, SD_SYNCPUBLISHJOB PJ 
                   WHERE P.PUBLISHID = PJ.PUBLISHID
                         AND (P.HOLDFLAG.string = '0' OR P.HOLDFLAG.string = '6' OR P.HOLDFLAG.string = '24' OR P.HOLDFLAG.string = '20' OR P.HOLDFLAG.string = '11')
                         AND (PJ.IGNOREFLAG.string = '0' OR PJ.IGNOREFLAG.string = '8')
                         AND PJ.PUBLISHFINALSTATUSCD.string = 'DONE'
                         AND (PJ.DELTATYPECD = P.DELTATYPECD OR UPDATESTRATEGY.string = 'FL')"""

    val KPI_3 = """SELECT
                                SJ.TARGETOBJECTDESC.string AS TARGETOBJECTDESC,
                                SJ.PUBLISHID.string AS PUBLISHID,
                                SJ.PUBLISHJOBID.string AS PUBLISHJOBID,
                                SJ.SUBSCRIBEJOBID.string AS SUBSCRIBEJOBID,
                                SJ.SOURCESYSTEMCD.string AS SOURCESYSTEMCD,
                                SJ.TARGETSYSTEMCD.string AS TARGETSYSTEMCD,
                                PJ.STARTDELTADESCVAL.string AS STARTDELTADESCVAL,
                                PJ.ENDDELTADESCVAL.string AS ENDDELTADESCVAL,
                                SJ.IGNOREFLAG.string AS IGNOREFLAG,
                                SJ.DELTATYPECD.string AS DELTATYPECD,
                                PJ.DELTA_SIZE AS DELTA_SIZE,
                                P.UPDATESTRATEGY.string AS UPDATESTRATEGY,
                                SJ.INSERTTS.string AS INSERTTS,
                                SJ.UPDATETS.string AS UPDATETS,
                                to_date(from_unixtime(unix_timestamp()))  AS REPORTDATE,
                                from_unixtime(unix_timestamp()) AS CRE_TS,
                                cast(NULL as timestamp) AS UPD_TS
                           FROM SD_SYNCPUBLISH P, SD_SYNCPUBLISHJOB PJ, SD_SYNCSUBSCRIBE S, SD_SYNCSUBSCRIBEJOB SJ
                           WHERE P.PUBLISHID = PJ.PUBLISHID
                                AND (P.HOLDFLAG.string = '0' OR P.HOLDFLAG.string = '6' OR P.HOLDFLAG.string = '24' OR P.HOLDFLAG.string = '20' OR P.HOLDFLAG.string = '11')
                                AND (PJ.IGNOREFLAG.string = '0' OR PJ.IGNOREFLAG.string = '8') AND PJ.PUBLISHFINALSTATUSCD.string = 'DONE'
                                AND PJ.PUBLISHID = S.PUBLISHID
                                AND (S.HOLDFLAG.string = '0' OR S.HOLDFLAG.string = '6' OR S.HOLDFLAG.string = '24' OR S.HOLDFLAG.string = '20' OR S.HOLDFLAG.string = '11')
                                AND S.SUBSCRIBEID = SJ.SUBSCRIBEID
                                AND S.PUBLISHID = SJ.PUBLISHID
                                AND PJ.PUBLISHJOBID = SJ.PUBLISHJOBID
                                AND (PJ.DELTATYPECD = P.DELTATYPECD OR UPDATESTRATEGY.string = 'FL')"""

    val KPI_4_1 = """SELECT
                         PJ.OBJECTDESC.string AS OBJECTDESC,
                         SJ.PUBLISHID.string AS PUBLISHID,
                         SJ.PUBLISHJOBID.string AS PUBLISHJOBID,
                         SJ.SUBSCRIBEJOBID.string AS SUBSCRIBEJOBID,
                         SJ.SOURCESYSTEMCD.string AS SOURCESYSTEMCD,
                         SJ.TARGETSYSTEMCD.string AS TARGETSYSTEMCD,
                         P.HOLDFLAG.string AS SOURCEHOLDFLAG,
                         S.HOLDFLAG.string AS TARGETHOLDFLAG,
                         PJ.DELTATYPECD.string AS DELTATYPECD,
                         P.UPDATESTRATEGY.string AS UPDATESTRATEGY,
                         PJ.BUILDDELTASTARTTS.string AS BUILDDELTASTARTTS,
                         PJ.BUILDDELTASTATUSTS.string AS BUILDDELTASTATUSTS,
                         (unix_timestamp(cast(BUILDDELTASTATUSTS.string as timestamp)) - unix_timestamp(cast(BUILDDELTASTARTTS.string as timestamp))) AS BUILDDELTA_TIME,
                         PJ.EXTRACTDATASTARTTS.string AS EXTRACTDATASTARTTS,
                         PJ.EXTRACTDATASTATUSTS.string AS EXTRACTDATASTATUSTS,
                         (unix_timestamp(cast(EXTRACTDATASTATUSTS.string as timestamp)) - unix_timestamp(cast(EXTRACTDATASTARTTS.string as timestamp))) AS EXTRACT_TIME,
                         SJ.LOADDATASTARTTS.string AS LOADDATASTARTTS,
                         SJ.LOADDATASTATUSTS.string AS LOADDATASTATUSTS,
                         (unix_timestamp(cast(LOADDATASTATUSTS.string as timestamp)) - unix_timestamp(cast(LOADDATASTARTTS.string as timestamp))) AS LOAD_TIME,
                         COALESCE(CAST(SRCLASTREFRESHTS as timestamp), CAST(ENDDELTADESCVAL.string AS timestamp)) AS SRCREFRESHTS,                                
                         SJ.APPLYSTARTTS.string AS APPLYSTARTTS,
                         SJ.APPLYSTATUSTS.string AS APPLYSTATUSTS,
                         (unix_timestamp(cast(APPLYSTATUSTS.string as timestamp)) - unix_timestamp(cast(APPLYSTARTTS.string as timestamp))) AS APPLY_TIME,
                         SJ.SUBSCRIBEFINALSTATUSTS AS SUBSCRIBEFINALSTATUSTS,
                         (unix_timestamp(cast(SUBSCRIBEFINALSTATUSTS.string as timestamp)) - unix_timestamp(cast(BUILDDELTASTARTTS.string as timestamp))) AS TOTAL_JOB_TIME,     
                         PJ.STARTDELTADESCVAL.string AS STARTDELTADESCVAL,
                         PJ.ENDDELTADESCVAL.string AS ENDDELTADESCVAL,
                         PJ.INSERTTS.string AS INSERTTS,
                         PJ.UPDATETS.string AS UPDATETS,
                         to_date(from_unixtime(unix_timestamp()))  AS REPORTDATE,
                         from_unixtime(unix_timestamp()) AS CRE_TS,
                         cast(NULL as timestamp) AS UPD_TS
                    FROM  SD_SYNCPUBLISH P,
                          SD_SYNCSUBSCRIBE S,
                          SD_SYNCPUBLISHJOB PJ,
                          SD_SYNCSUBSCRIBEJOB SJ
                    WHERE P.PUBLISHID = S.PUBLISHID
                                  AND P.HOLDFLAG.string = '0'
                                  AND S.HOLDFLAG.string = '0'
                                  AND P.PUBLISHID = PJ.PUBLISHID
                                  AND (PJ.IGNOREFLAG.string = '0' OR PJ.IGNOREFLAG.string = '8') AND PJ.PUBLISHFINALSTATUSCD.string = 'DONE'
                                  AND S.SUBSCRIBEID = SJ.SUBSCRIBEID
                                  AND S.PUBLISHID = SJ.PUBLISHID
                                  AND PJ.PUBLISHJOBID = SJ.PUBLISHJOBID
                                  AND (SJ.IGNOREFLAG.string = '0' OR SJ.IGNOREFLAG.string = '8') AND SJ.SUBSCRIBEFINALSTATUSCD.string = 'DONE'
                                  AND (PJ.DELTATYPECD = P.DELTATYPECD OR UPDATESTRATEGY.string = 'FL')
                                  AND APPLYSTARTTS.string >  '2000-01-01 00:00:00'"""

    val KPI_4_2 = """ SELECT
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
                                  ELSE (unix_timestamp(cast(SUBSCRIBEFINALSTATUSTS.string AS timestamp)) - unix_timestamp(cast(SRCREFRESHTS as timestamp))) END AS TOT_REPLICATION_TIME,
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
                             TOT_REPLICATION_TIME  AS TOTAL_REPLICATION_SECS,
                             INSERTTS,
                             UPDATETS,
                             REPORTDATE,
                             CRE_TS,
                             UPD_TS
                       FROM SD_FACT_REPLICATION_LATENCY_v02"""

    val KPI_5 = """SELECT 
                         REPORTDATE, 
                         SUM(TOTAL_REPLICATION_SECS)  / (60 * COUNT(*))  AS TOTAL_REP_MINUTES
                   FROM (
                         SELECT
                               TOT_REPLICATION_TIME,
                               TOTAL_REPLICATION_SECS,
                               REPORTDATE
                         FROM SD_FACT_REPLICATION_LATENCY_v03
                         WHERE TOT_REPLICATION_TIME IS NOT NULL AND TOTAL_REPLICATION_SECS<600000
                        ) A GROUP BY REPORTDATE """
    /**/

    val KPI_6 = """SELECT 
                         REPORTDATE,
                         SOURCESYSTEMCD, 
                         COUNT(DISTINCT OBJECTDESC) AS TOTAL_SOURCE_TABLES, 
                         COUNT(*) TOTAL_REPLICATIONS 
                   FROM SD_FACT_OBJECTS_ENABLED_v01
                   WHERE REPORTDATE = '2015-07-28'
                   GROUP BY REPORTDATE, SOURCESYSTEMCD 
                   ORDER BY  TOTAL_SOURCE_TABLES DESC"""

    val KPI_7 = """SELECT 
                         REPORTDATE,
                         SUM(CAST(SOURCEDELTASIZE as BIGINT)/(1024*1024*1024))  
                   FROM SD_FACT_SOURCE_DELTASIZE_v01
                   WHERE (IGNOREFLAG = '0' OR IGNOREFLAG = '8') 
                         AND INSERTTS BETWEEN '2015-07-01' AND '2015-07-30' 
                         GROUP BY REPORTDATE"""
/// EXTRACT(DAY FROM (LAST_DAY(CURRENT_DATE)))
    
    /*val KPI_8 = """SELECT 
                        REPORTDATE,
                        TARGETSYSTEMCD,
                        TO_CHAR(SUM(DELTASIZE/(1024*1024*1024)),'FM999999999.90')/ EXTRACT(DAY FROM (LAST_DAY(CAST('2015-06-30' AS DATE))))
                   FROM  SD_FACT_TARGET_DELTASIZE_v01
                   GROUP BY REPORTDATE, TARGETSYSTEMCD"""
    */
    
    val result_1 = hiveContext.sql(KPI_1)
    result_1.registerTempTable("SD_FACT_OBJECTS_ENABLED_v01")
    result_1.collect()
    //result_1.saveAsAvroFile("/Users/abbhatnagar/qwerty/")
    result_1.show()
    result_1.saveAsTable("SD_FACT_OBJECTS_ENABLED_vt01", SaveMode.Append)


    val result_2 = hiveContext.sql(KPI_2)
    result_2.registerTempTable("SD_FACT_SOURCE_DELTASIZE_v01")
    result_2.collect()
    result_2.show()
    result_2.saveAsTable("SD_FACT_SOURCE_DELTASIZE_vt01", SaveMode.Append)

//    val count2 = hiveContext.sql("Select count(*) from SD_FACT_SOURCE_DELTASIZE_v01")
//    count2.show()

    val result_3 = hiveContext.sql(KPI_3)
    result_3.registerTempTable("SD_FACT_TARGET_DELTASIZE_v01")
    result_3.collect()
    result_3.show()
    result_3.saveAsTable("SD_FACT_TARGET_DELTASIZE_vt01", SaveMode.Append)
    
//    val count3 = hiveContext.sql("Select count(*) from SD_FACT_TARGET_DELTASIZE_v01")
//    count3.show()

    val result_4_1 = hiveContext.sql(KPI_4_1)
    result_4_1.registerTempTable("SD_FACT_REPLICATION_LATENCY_v01")
    result_4_1.collect()
    val result_4_2 = hiveContext.sql(KPI_4_2)
    result_4_2.registerTempTable("SD_FACT_REPLICATION_LATENCY_v02")
    result_4_2.collect()
    val result_4 = hiveContext.sql(KPI_4_3)
    result_4.registerTempTable("SD_FACT_REPLICATION_LATENCY_v03")
    result_4.collect()
    result_4.show()
    result_4.saveAsTable("SD_FACT_REPLICATION_LATENCY_vt03", SaveMode.Append)
    
    val result_5 = hiveContext.sql(KPI_5)
    result_5.registerTempTable("SD_AVG_TIME_PROC_v01")
    result_5.collect()
    result_5.show()
    result_5.saveAsTable("SD_AVG_TIME_PROC_vt01", SaveMode.Append)

    val result_6 = hiveContext.sql(KPI_6)
    result_6.registerTempTable("SD_DATASET_SOURCE_ENABLED_v01")
    result_6.collect()
    result_6.show()
    result_6.saveAsTable("SD_DATASET_SOURCE_ENABLED_vt01", SaveMode.Append)

    val result_7 = hiveContext.sql(KPI_7)
    result_7.registerTempTable("SD_DATA_SOURCE_REPLICATE_v01")
    result_7.collect()
    result_7.show()
    result_7.saveAsTable("SD_DATA_SOURCE_REPLICATE_vt01", SaveMode.Append)



    //result_1.saveAsTable("SD_FACT_REPLICATION_LATENCY_v05", SaveMode.Append)

    val mydata = hiveContext.table("SD_FACT_OBJECTS_ENABLED_vt01")
    mydata.show()

  }
}




    //    val result_8 = hiveContext.sql(KPI_8)
    //    result_8.registerTempTable("SD_DATA_TARGET_REPLICATE_v01")
    //    result_8.collect()
    //    result_8.show()



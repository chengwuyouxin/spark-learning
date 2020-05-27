package com.lpq.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/*
DataFrame的基本使用方法
 */

object DataFrameDemo {
  def main(args:Array[String]): Unit ={

    val spark=SparkSession
      .builder()
      .config("hive.metastore.uris","thrift://22.11.97.142:9083")
      .appName("DataFrameDemo")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._;

    spark.sql("select * from bdp.INNER_ODS_BSMS_TRANSACTION_JOURNAL where day='20190412'")
      .createOrReplaceTempView("bsms_journal")

    spark.sql("select branchid,tellerid,customerno," +
      "ceil((unix_timestamp(max(modifiedtime))-unix_timestamp(min(modifiedtime)))/60) as eclipsetime " +
      "from bsms_journal " +
      "where substr(translate(trim(ModifiedTime),'-',''),1,8)='20190120' " +
      "and day='20190120' and customerno !='' " +
      "group by branchid,tellerid,customerno " +
      "order by eclipsetime desc")

    spark.sql("insert into aomds.demo_result partition(day='20190412') select branchid,tellerid,customerno," +
      "ceil((unix_timestamp(max(modifiedtime))-unix_timestamp(min(modifiedtime)))/60) as eclipsetime " +
      "from bdp.INNER_ODS_BSMS_TRANSACTION_JOURNAL " +
      "where substr(translate(trim(ModifiedTime),'-',''),1,8)='?' " +
      "and day='?' and customerno !='' " +
      "group by branchid,tellerid,customerno " +
      "order by eclipsetime desc")

    //读取hive表
    val bsms_journal=spark.table("analyze.bsms_journal");

    bsms_journal.persist(StorageLevel.MEMORY_ONLY_SER)

    val res=bsms_journal.filter("customerno!='' and substr(translate(trim(ModifiedTime),'-',''),1,8)='20190120'")
      .groupBy("branchid","tellerid","customerno")
      .agg("modifiedtime"-> "max","modifiedtime"->"min")
      .withColumnRenamed("max(modifiedtime)","max")
      .withColumnRenamed("min(modifiedtime)","min")
      .selectExpr("branchid","tellerid","customerno","ceil((unix_timestamp(max)-unix_timestamp(min))/60) as eclipsetime")
      .orderBy($"eclipsetime".desc)

    res.createOrReplaceTempView("res")
    spark.sql("insert into aomds.demo_result partition(day='20190412') select * from res")


  }
}

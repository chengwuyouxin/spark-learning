package com.lpq.spark.sql

import org.apache.spark.sql.SparkSession

object SparkSqlDemo {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .config("hive.metastore.uris","thrift://192.168.29.149:9083")
      .enableHiveSupport()
      .getOrCreate()

    //使用spark存储metadata
//    val student = Seq(("lpq",29),("xmm",31))
//    val studentdf = spark.createDataFrame(student).toDF("name","age")
//    studentdf.createOrReplaceTempView("student")
//    spark.sql("select name from student where age>30").show()
//    spark.sql("select name from student where age>30").explain(true)

    //使用hive存储metadata
    spark.sql("insert into test.result select a.name,a.age,b.score " +
      "from test.student a join test.exam b on a.name = b.name")
    spark.sql("select * from test.result").show()

    val parser = spark.sessionState.sqlParser


  }

}

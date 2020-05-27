package com.lpq.spark.core.listener

import org.apache.spark.sql.SparkSession

/*
设置Spark listener的方法
1.  config("spark.extraListeners","sparklistenerdemo.MySparkAppListener")
2.  spark.sparkContext.addSparkListener(new MySparkAppListener)

设置Spark sql listener
1. config("spark.sql.queryExecutionListeners","sparklistenerdemo.MyQueryExecutionListener") 不起作用
2. spark.listenerManager.register(new MyQueryExecutionListener())
 */
object Main{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Listener Demo")
      .enableHiveSupport()
      .getOrCreate()

    spark.listenerManager.register(new MyQueryExecutionListener())

    val user = Seq(("lpq",29),("xmm",31))
    val userdf = spark.createDataFrame(user).toDF("name","age")

    userdf.createOrReplaceTempView("user")


    spark.sql("select * from user where age>30").show()

    spark.stop()
  }
}

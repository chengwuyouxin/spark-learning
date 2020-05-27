package com.lpq.spark.sql.lineage.table

import org.apache.spark.sql.SparkSession

/**
  * @author liupengqiang
  * @date 2020/5/12
  */
object TableLineage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("hive.metastore.uris","thrift://192.168.29.149:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.listenerManager.register(new SparkSqlLineageListener())

//    spark.sql("insert into test.result select a.name,a.age,b.score " +
//      "from test.student a join test.exam b on a.name = b.name")

    val sql = "insert into test.result select a.name,a.age,b.score " +
      "from test.student a join test.exam b on a.name = b.name";

//    val parser = spark.sessionState.sqlParser
//    val unresolved_lp = parser.parsePlan(sql)
//
//    val qe =spark.sessionState.executePlan(unresolved_lp)
//    val analyzed_lp = qe.analyzed
//
//    print(analyzed_lp.getClass)

    spark.sql(sql)

    spark.stop()
  }
}

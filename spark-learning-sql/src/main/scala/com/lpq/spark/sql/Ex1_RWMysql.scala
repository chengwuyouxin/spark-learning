package com.lpq.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
Spark sql读写mysql
 */

object Ex1_RWMysql {
  def main(args:Array[String]): Unit ={
    val spark=SparkSession.builder()
      .appName("Ex1_RWMysql")
      .getOrCreate()
    val rdd:RDD[Person]=spark.sparkContext.makeRDD(Seq(Person("A",10),Person("B",10)))
    val df:DataFrame=spark.createDataFrame(rdd)

    //写入Mysql
    //方法1：
    df.write
      .mode("append")
      .format("jdbc")
      .option("url","jdbc:mysql://22.11.97.142:3306/lpq")
      .option("dbtable","result")
      .option("user","root")
      .option("password","admin")
      .save()

    //方法2:
    //    val properties=new Properties()
    //    properties.put("user","root");
    //    properties.put("password","admin")
    //    df.write
    // .mode("append")
    // .jdbc("jdbc:mysql://22.11.97.142:3306/lpq","result",properties)

    //读取Mysql
    //方法1:
    //    val df2=spark.read
    //      .format("jdbc")
    //      .option("url","jdbc:mysql://22.11.97.142:3306/lpq")
    //      .option("dbtable","result")
    //      .option("user","root")
    //      .option("password","admin")
    //      .load()
    //
    //    //方法2：
    //    val properties=new Properties();
    //    properties.put("user","root")
    //    properties.put("password","admin")
    //    val df2=spark.read.jdbc("jdbc:mysql://22.11.97.142:3306/lpq","result",properties)
    //    df.show()
  }
}
case class Person(name:String,age:Int)
package com.lpq.spark.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args:Array[String]): Unit ={
    val conf=new SparkConf().setAppName("WordCount")
    val sc=new SparkContext(conf)

    val rdd=sc.parallelize(Array("a c","a b","b c","b d","c d"),2)
    val word=rdd.flatMap(_.split(" "))
    val wordpair=word.map((_,1))
    val wordCount=wordpair.reduceByKey(_+_)

    wordCount.collect().foreach(println)

    sc.stop()
  }
}

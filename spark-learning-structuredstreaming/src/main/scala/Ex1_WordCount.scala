import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

object Ex1_WordCount {
def main(args:Array[String]): Unit ={
    val spark=SparkSession.builder()
      .appName("Ex1_WordCount")
      .getOrCreate()

    import spark.implicits._

    //structruedstreaming会为每个查询随机创建一个kafka消费者群组名
    //structuredstreaming自己管理offset
    val stream=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","dn01:9092,dn02:9092,dn03:9092")
      .option("subscribe","wordcount")
//      .option("startingOffsets","latest") //应用每次启动时都会从应用启动后kafka收到的最新消息开始消费
      .option("startingOffsets","earliest") //应用每次启动时都会从最开始的消息开始消费
      .option("endingOffsets","latest")
      .load()

    import org.apache.spark.sql.functions.window

    /*
    structuredstreaming从kafka获取的消息结构为
    key binary
    value binary
    topic string
    partition int
    offset  long
    timestamp long  kafka收到该消息的时间
    timestampType int
    */

//    val line=stream.selectExpr(
//        "CAST(key AS String)",
//        "CAST(value AS STRING)",
//        "topic",
//        "partition",
//        "offset",
//        "timestamp",
//        "timestampType")
//      .as[(String,String,String,Int,Long,Long,Int)]
//    val query=line.writeStream
//        .outputMode("append")  //默认是append模式
//        .format("console")
//        .start()

/*
    +---+-------+---------+---------+------+--------------------+-------------+
    |key|  value|    topic|partition|offset|           timestamp|timestampType|
    +---+-------+---------+---------+------+--------------------+-------------+
    |  1|  hbase|wordcount|        0|     0|2019-07-26 10:27:...|            0|
    |  2|  redis|wordcount|        0|     1|2019-07-26 10:27:...|            0|
    |  3|  hbase|wordcount|        0|     2|2019-07-26 10:27:...|            0|
*/

    //普通aggregation操作,此时输出只能是Complete或者Update模式,不支持Append模式
//    val windowCounts=stream.selectExpr("CAST(value AS STRING)","timestamp").as[(String,Long)]
//      .groupBy(window($"timestamp","10 seconds","5 seconds"),$"value")
//      .count()
//    val query=windowCounts.writeStream
//      .trigger(ProcessingTime("10 seconds"))
//      .outputMode(OutputMode.Complete())
//      .format("console")
//      .start()

    /*
+--------------------+-------+-----+
|              window|  value|count|
+--------------------+-------+-----+
|[2019-07-26 10:32...|mongodb|    2|
|[2019-07-26 10:31...|  redis|    2|
|[2019-07-26 10:33...|  redis|    1|
     */

    //带有WaterMark的聚合操作，输出支持Complete/Update/Append三种模式
//    val windowCountsWithWaterMark=stream
//      .selectExpr("CAST(value AS STRING)","timestamp")
//      .as[(String,Long)]
//      .withWatermark("timestamp","10 seconds")
//      .groupBy(window($"timestamp","10 seconds","5 seconds"),$"value")
//      .count()
//    val query=windowCountsWithWaterMark.writeStream
//      .trigger(ProcessingTime("10 seconds"))
//      .outputMode("append")
//      .format("console")
//      .start()
/*
+--------------------+-------+-----+
|              window|  value|count|
+--------------------+-------+-----+
|[2019-07-26 10:32...|mongodb|    2|
|[2019-07-26 10:31...|  redis|    2|
|[2019-07-26 10:33...|  redis|    1|
*/

    val ds1=stream.selectExpr("CAST(value AS STRING)","timestamp").as[(String,Long)]
    ds1.printSchema()
/*
root
 |-- value: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)
*/
    val ds2=ds1.withWatermark("timestamp","10 seconds")
    ds2.printSchema()
/*
root
 |-- value: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 */
    val windowCountsWithWaterMark=ds2
      .groupBy(window($"timestamp","10 seconds","5 seconds"),$"value")
      .count()
    windowCountsWithWaterMark.printSchema()
/*
root
 |-- window: struct (nullable = true)
 |    |-- start: timestamp (nullable = true)
 |    |-- end: timestamp (nullable = true)
 |-- value: string (nullable = true)
 |-- count: long (nullable = false)
*/
    val query=windowCountsWithWaterMark.writeStream
      .trigger(ProcessingTime("10 seconds"))
      .outputMode("append")
      .format("console")
      .start()

    //file sink:支持text/json/parquet/csv,需要设置checkpointLocation和path,outputmode只支持Append
//    val windowCountsWithWaterMark=stream.selectExpr("CAST(value AS STRING)","timestamp").as[(String,Long)]
//      .withWatermark("timestamp","10 seconds")
//      .groupBy(window($"timestamp","10 seconds","5 seconds"),$"value")
//      .count()
//    val query=windowCountsWithWaterMark.writeStream
//      .outputMode("append")
//      .format("parquet")
//      .option("checkpointLocation","E://structuredstreaming//checkpoint//wordcount")
//      .option("path","E://structuredstreaming//parquetoutput")
//      .start()

    //foreach sink:自定义了Mysql/Redis sink
//    val windowCountsWithWaterMark=stream.selectExpr("CAST(value AS STRING)","timestamp").as[(String,Long)]
//      .withWatermark("timestamp","10 seconds")
//      .groupBy(window($"timestamp","10 seconds","5 seconds"),$"value")
//      .count()
//    val query=windowCountsWithWaterMark.writeStream
//      .trigger(ProcessingTime("10 seconds"))
//      .outputMode(OutputMode.Complete())
////      .foreach(new RedisSink)
//      .foreach(new MysqlSink)
//      .start()

    //Exactly-once: structuredstreaming通过checkpoint来保存中间状态，
    //程序宕机重启时候会从checkpoint读取中间状态进行恢复,
    //但是如下测试发现，每次程序重启时候都会重复处理10条数据，也就是一个trigger的数据
//    val windowCounts=stream
//         .selectExpr("timestamp","CAST(value AS STRING)")
//         .as[(Long,String)]
//         .select($"timestamp",$"value")
//    val query=windowCounts
//          .writeStream
//          .option("checkpointLocation","E://structuredstreaming//checkpoint//wordcount//")
//          .trigger(ProcessingTime("10 seconds"))
//          .outputMode(OutputMode.Append())
//          .foreach(new MysqlSink)
//          .start()

    query.awaitTermination()
  }
}

case class OrgLevelRecord(value:String,orgleveltime:Long)
import org.apache.spark.sql.SparkSession

object Ex4_DstreamJoinDataframe {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ex3_DstreamJoinDstream")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val orglevelStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092")
      .option("subscribe", "orglevel")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    val bsmsTransLogDF = spark.sparkContext.textFile("hdfs://nn01:8020/data/bsms")
      .map(_.split("\\|"))
      .map(record => BsmsTrans(record(0), record(1), record(2),
        record(3), record(4), record(5),
        record(6), record(7), record(8),
        record(9), record(10), record(11),
        record(12), record(13), record(14),
        record(15), record(16), record(17),
        record(18), record(19), record(20),
        record(21), record(22), record(23),
        record(24), record(25), record(26),
        record(27), record(28), record(29),
        record(30), record(31), record(32),
        record(33), record(34), record(35),
        record(36), record(37), record(38))).toDF()

    val orglevelDF=orglevelStream.selectExpr("CAST(value AS STRING)", "timestamp")
      .as[(String,Long)]
      .withColumn("value",split(col("value"),"\\|"))
      .select(
        $"value".getItem(0).as("ProvBankCode"),
        $"value".getItem(1).as("ProvBankName"),
        $"value".getItem(2).as("SndBranchID"),
        $"value".getItem(3).as("SndBranchName"),
        $"value".getItem(4).as("AdmBranchID"),
        $"value".getItem(5).as("AdmBranchName"),
        $"value".getItem(6).as("BranchID"),
        $"value".getItem(7).as("BranchName"),
        $"value".getItem(8).as("Day"),
        $"timestamp"
      ).withWatermark("timestamp","5 seconds")
      .join(bsmsTransLogDF, "BranchId")
      .groupBy(window($"timestamp","10 seconds","5 seconds"),$"BranchID")
      .count()

    val query = orglevelDF.writeStream
      .outputMode("append")
      .format("console")
      .start()
/*
+--------------------+--------+-----+
|              window|BranchID|count|
+--------------------+--------+-----+
|[2019-07-30 10:07...|   14545|  153|
|[2019-07-30 10:07...|   14329|  561|
|[2019-07-30 10:07...|   14621|    5|
|[2019-07-30 10:07...|   14632|  156|
|[2019-07-30 10:07...|   14420| 3016|
|[2019-07-30 10:07...|   14498| 2540|
|[2019-07-30 10:07...|   14417| 1627|
|[2019-07-30 10:07...|   14525|    3|
|[2019-07-30 10:07...|   14538| 1563|
|[2019-07-30 10:07...|   14517| 1526|
|[2019-07-30 10:07...|   14532| 2400|
 */

    query.awaitTermination()
  }
}




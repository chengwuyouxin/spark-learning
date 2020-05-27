import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Ex9_DstreamJoinDataFrame {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DstreamJoinDstream")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //程序关闭时会先处理完接受到的数据
      .set("spark.streaming.backpressure.enabled", "true") //激活削峰功能,sparkstreaming可以根据系统处理
      .set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值，需要设置backpressure为true
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaProps = Map[String, Object](
      "bootstrap.servers" -> "dn01:9092,dn02:9092,dn03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "orglevel",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = Array("orglevel")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaProps)
    )

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val orgLevelDF = rdd.map(_.value)
        .map(_.split("\\|"))
        .map(record => OrgLevel(record(0), record(1), record(2),
          record(3), record(4), record(5),
          record(6), record(7), record(8))).toDF()

      val bsmsTransDF = spark.sparkContext.textFile("hdfs://nn01:8020/data/bsms")
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

      println("orgleveldf:"+orgLevelDF.count())
      println("bsmsTransDF:"+bsmsTransDF.count())
      val res = orgLevelDF.join(bsmsTransDF, "BranchID")
        .groupBy(orgLevelDF("BranchID"))
        .count()
      res.show()



      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

case class BsmsTrans(IncFlg:String,ModifiedDate:String,ProvBankCode:String,
                     TransactionJournalID:String,TransactionNo:String,TellerID:String,
                     BranchID:String,TransactionGroupID:String,TransactionGroupName:String,
                     ModifiedTime:String,ProductNoForOcrm:String,
                     Currency:String,TranMarketRelationID:String,
                     ServiceID:String,OutAmount:String,OutCurrency:String,
                     ProductSubType:String,flag1:String,flag2:String,flag3:String,
                     flag4:String,flag5:String,EntryTransactionNo:String,
                     ISCALL:String,TICKETNUMBER:String,TAKETYPE :String,
                     CUSTTYPE:String,CUSTLVL :String,TRANTOPID:String,
                     TRANTYPEID:String,ARRIVETIME:String,BOOKWAY:String,
                     WINID:String,ISARRIVE :String,
                     TOWINID :String,Appraisal:String,UnsatisfactoryCode:String,
                     TRANKIND:String,Day:String)

case class OrgLevel(ProvBankCode:String,ProvBankName:String,SndBranchID:String,
                      SndBranchName:String,AdmBranchID:String,AdmBranchName:String,
                      BranchID:String,BranchName:String,day:String)
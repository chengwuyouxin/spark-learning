import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}


//sparkstreaming无法实现真正意义上的多个Dstream的join，只能通过订阅多个topic形成一个Dstream
//然后通过过滤拆分成多个DataFrame,在进行join

object Ex7_DstreamJoinDstream {
  def main(args:Array[String]): Unit ={
    val conf=new SparkConf()
      .setAppName("DstreamJoinDstream")
      .set("spark.streaming.stopGracefullyOnShutdown","true")//程序关闭时会先处理完接受到的数据
      .set("spark.streaming.backpressure.enabled", "true") //激活削峰功能,sparkstreaming可以根据系统处理
      .set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值，需要设置backpressure为true
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val ssc=new StreamingContext(conf,Seconds(5))

    val kafkaProps=Map[String,Object](
      "bootstrap.servers" -> "dn01:9092,dn02:9092,dn03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "facerecglog",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    val topic=Array("facerecglog","transactionlog")
    val stream=KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](topic,kafkaProps)
    )

    stream.foreachRDD{ rdd =>
      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val spark=SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val faceDF=rdd.filter(record => record.key().startsWith("facerecglog"))
        .map(_.value)
        .map(_.split("\\|"))
        .map(record => FaceRecgLog(record(0),record(1),record(2),
          record(3),record(4),record(5),
          record(6),record(7),record(8),
          record(9),record(10),record(11),
          record(12),record(13),record(14),
          record(15),record(16),record(17),
          record(18),record(19),record(20),
          record(21),record(22),record(23),
          record(24),record(25),record(26),
          record(27),record(28),record(29),
          record(30),record(31))).toDF()

      val transDF=rdd.filter(record => record.key().startsWith("transactionlog"))
        .map(_.value)
        .map(_.split("\\|"))
        .map(record => TransactionLog(record(0),record(1),record(2),
          record(3),record(4),record(5),
          record(6),record(7),record(8),
          record(9),record(10),record(11),
          record(12),record(13),record(14),
          record(15),record(16),record(17),
          record(18),record(19),record(20),
          record(21),record(22),record(23),
          record(24),record(25),record(26),
          record(27),record(28),record(29))).toDF()

      println("faceDF:"+faceDF.count())
      println("transDF:"+transDF.count())
      val res=faceDF.join(transDF,"ScenarioInstanceId").select(faceDF("IncreaseFlag"))
      res.show()

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
case class FaceRecgLog(IncreaseFlag:String,Date:String,LogDate:String,
                       LogTime:String,ProvBranchNoEHR:String,SupBranchNum:String,
                       BranchNo:String,BranchName:String,MachineId:String,
                       ScenarioNo:String,Trandesc:String,CustomerName:String,
                       IdentityType:String,IdentityNo:String,CustomerNo:String,
                       ImageType:String,ImageId:String,ImageDesc:String,
                       FaceRgResult:String,ErrorMessage:String,FaceRecgID:String,
                       ScenarioInstanceId:String,TaskType:String,DealerId:String,
                       PadResult:String,DBServerId:String,Aliveness1:String,
                       Aliveness1Result:String,Aliveness2:String,Aliveness2Result:String,
                       TaskId:String,Day:String)

case class TaskInfos(IncreaseFlag:String,Date:String,Logdate:String,
                     ProvBranchNoEHR:String,BranchNo:String,
                     BranchName:String,ScenarioName:String,CreateTime:String,
                     CreatorId:String,ReferenceData:String,CustomerNo:String,
                     IdentityType:String,CustomerName:String,IdentityNo:String,
                     ImageId:String,Title:String,Id:String,ExCheckResultDesc:String,
                     IsCustInfoBtnDesc:String,PIVSChkResultDesc:String,
                     UpdPivsRltDesc:String,DealerId:String,StatusText:String,
                     RejectReason:String,ExceptionMsg:String,CompRes:String,
                     CBIPRetCode:String,DealTime:String,SubType:String,Day:String)

case class TransactionLog(IncreaseFlag:String,Date:String,TransactionLogId:String,
                          ProvBranchNoEHR:String,SupBranchNum:String,BranchNo:String,
                          MachineId:String,StartTime:String,ScenarioNo:String,
                          ScenarioName:String,ScenarioInstanceId:String,
                          BusinessTypeNo:String,BusinessTypeName:String,UUID:String,
                          CustomerNo:String,CustomerName:String,IdentityType:String,
                          IdentityNo:String,CardNum:String,Amount:String,statusId:String,
                          TransactionMessage:String,ImageId:String,AgreementId:String,
                          ErrorMessage:String,LogTime:String,LogDate:String,
                          DBServerId:String,DBType:String,Day:String)

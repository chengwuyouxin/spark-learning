import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Ex10_DstreamJoinDstream {
   def main(args:Array[String]): Unit ={
    val conf=new SparkConf()
      .setAppName("DstreamJoinDstream")
      .set("spark.streaming.stopGracefullyOnShutdown","true")//程序关闭时会先处理完接受到的数据
      .set("spark.streaming.backpressure.enabled", "true") //激活削峰功能,sparkstreaming可以根据系统处理
      .set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值，需要设置backpressure为true
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val ssc=new StreamingContext(conf,Seconds(5))

    val kafkaProps1=Map[String,Object](
      "bootstrap.servers" -> "dn01:9092,dn02:9092,dn03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "facerecglog",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )
    val topic1=Array("facerecglog")
    val stream1=KafkaUtils.createDirectStream[String,String](
        ssc,
        PreferConsistent,
        Subscribe[String,String](topic1,kafkaProps1)
      ).map(_.value)
      .map(value => (value.split("\\|")(21),value))

     val kafkaProps2=Map[String,Object](
       "bootstrap.servers" -> "dn01:9092,dn02:9092,dn03:9092",
       "key.deserializer" -> classOf[StringDeserializer],
       "value.deserializer" -> classOf[StringDeserializer],
       "group.id" -> "transactionlog",
       "auto.offset.reset" -> "earliest",
       "enable.auto.commit" -> (false:java.lang.Boolean)
     )
     val topic2=Array("transactionlog")
     val stream2=KafkaUtils.createDirectStream[String,String](
         ssc,
         PreferConsistent,
         Subscribe[String,String](topic2,kafkaProps1)
       ).map(_.value)
       .map(value => (value.split("\\|")(10),value))



//     val res=stream1.transformWith(stream2,
//       (faceRDD:RDD[(String,Array[String])],logRDD:RDD[<String,String>]) ={
//       faceRDD.join(logRDD,)
//     })


//    stream1.foreachRDD{ rdd =>
//      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//    }

    ssc.start()
    ssc.awaitTermination()
  }
}

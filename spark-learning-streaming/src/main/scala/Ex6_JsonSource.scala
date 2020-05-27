import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Ex6_JsonSource {
  def main(args:Array[String]):Unit= {
    val conf = new SparkConf()
      .setAppName("CashPlanLoad")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //程序关闭时会先处理完接受到的数据
      .set("spark.streaming.backpressure.enabled", "true") //激活削峰功能,sparkstreaming可以根据系统处理
      //处理能力来调节获取的流量的大小，该流量的最大值由maxRatePerPartition来设置,一般都会小于该值
      .set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值，需要设置backpressure为true
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "dn01:9092,dn02:9092,dn03:9092",
      "key.deserializer" -> classOf[IntegerDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CPRConsumerGroup",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = Array("CashPlanRecord2")
    val stream = KafkaUtils.createDirectStream[Integer, String](
      ssc,
      PreferConsistent,
      Subscribe[Integer, String](topic, kafkaParams)
    )

    val schema=StructType(List(
      StructField("machineNo",StringType),
      StructField("termPaymentDay",StringType),
      StructField("currency",StringType),
      StructField("branchNo",StringType),
      StructField("tellerID",StringType),
      StructField("operTime",StringType),
      StructField("cashNum",IntegerType),
      StructField("faceValue",IntegerType))
    )

    stream.foreachRDD { rdd =>
      //获取topic的每个partition的偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //业务处理逻辑
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._;

      val value=rdd.map(_.value())
      val ds=spark.createDataset(value)
      val cashPlanDF=ds.select(from_json(($"value").cast("string"),schema).alias("value"))
        .select($"value.*")
      cashPlanDF.select($"currency",$"faceValue").show()


      //当处理完成后，手动提交偏移量
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

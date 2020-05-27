import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Ex11_Transform {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf()
      .setAppName("CashPlanLoad")
      .set("spark.streaming.stopGracefullyOnShutdown","true")//程序关闭时会先处理完接受到的数据
      .set("spark.streaming.backpressure.enabled", "true") //激活削峰功能,sparkstreaming可以根据系统处理
      //处理能力来调节获取的流量的大小，该流量的最大值由maxRatePerPartition来设置,一般都会小于该值
      .set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值，需要设置backpressure为true
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.mongodb.input.uri","mongodb://22.11.97.142:27017/lpq.cashplanrecord")
      .set("spark.mongodb.output.uri","mongodb://22.11.97.142:27017/lpq.cashplanrecord")

    val ssc=new StreamingContext(conf,Seconds(5))

    val kafkaParams=Map[String,Object](
      "bootstrap.servers"->"dn01:9092,dn02:9092,dn03:9092",
      "key.deserializer" -> classOf[IntegerDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CPRConsumerGroup",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    val topic:Array[String] = Array("CashPlanRecord")
    val stream:InputDStream[ConsumerRecord[Integer,String]]=KafkaUtils
      .createDirectStream[Integer,String](
      ssc,
      PreferConsistent,
      Subscribe[Integer,String](topic,kafkaParams)
    )

    //在transform中我们可以对rdd进行转换操作
    //但是不能破坏rdd和kafka分区的对应关系，因为我们最后要提交offset
    //map/mapPartitions这样的算子是安全的,shuffle或者repartition的算子是不安全的
    val offsetRanges:Array[OffsetRange]=Array.empty[OffsetRange]
    stream.transform{rdd =>
      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.mapPartitions(records => {
      val splitedRecords=records.map(_.value).map(_.split(","))
      splitedRecords
    }).foreachRDD{ rdd =>
      val spark=SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val cashPlanDF=rdd.map(array => CashPlanRecord2(array(0),array(1),array(2),array(3),array(4),array(5),
        array(6),array(7),array(8))).toDF()

      //结果写入MongoDB
      println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date) +":开始写入MongoDB")
      cashPlanDF.write.mode("append").mongo()
      println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date) +":成功写入MongoDB")

      //当处理完成后，手动提交偏移量
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }


    ssc.start()
    ssc.awaitTermination()
  }
}

//case class CashPlanRecord2(_id:String,MachineNo:String,TermPaymentDay:String,
//                           Currency:String,BranchNo:String,BLOperTellerID:String,
//                           BLOperTime:String,CashNum:String,FaceValue:String)
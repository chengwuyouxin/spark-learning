import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/*
保证MongoDB幂等性的方法：
MongoDB中存储的文档都有一个"_id"键，这个键可以是任意类型，如果不指定，则驱动程序会自动创建一个ObjectId对象。
sparkstreaming向MongoDB写入的对象时，指定了"_id"键，这样如果写入的记录重复，则会更新有相同"_id"的记录.
这样当sparkstreaming重复消费kafka中的消息时，写入MongoDB中相同的结果也能保证MongoDB的幂等性

使用kafka保存offset的问题：
如果在写入MongoDB时连接超时，此时仍然会提交offset到10，导致丢失了从0-10的数据
 */

object Ex5_Save2Mongo {
    def main(args:Array[String]):Unit={
    val conf=new SparkConf()
      .setAppName("CashPlanLoad")
      .set("spark.streaming.stopGracefullyOnShutdown","true")//程序关闭时会先处理完接受到的数据
      .set("spark.streaming.backpressure.enabled", "true") //激活削峰功能,sparkstreaming可以根据系统处理
      //处理能力来调节获取的流量的大小，该流量的最大值由maxRatePerPartition来设置,一般都会小于该值
      .set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值，需要设置backpressure为true
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//      .set("spark.mongodb.input.uri","mongodb://22.11.97.142:27017/lpq.cashplanrecord")
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

    //sparkstreaming通过Direct方式创建DStream，其rdd的partition数量和topic的partition数量一样
    stream.foreachRDD{ rdd:RDD[ConsumerRecord[Integer,String]] =>
      //获取topic的每个partition的偏移量
      val offsetRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition{ iter=>
        val o:OffsetRange=offsetRanges(TaskContext.get.partitionId())
        println("Kafka记录分区等信息：" + o.topic+","+o.partition+","
          +o.fromOffset+","+o.untilOffset)
      }

      //业务处理逻辑
      println("rdd的分区数量:"+rdd.partitions.size)
      val spark=SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val cashPlanDF=rdd.map(_.value)
        .map(value => value.split(","))
        .map(array => CashPlanRecord2(array(0),array(1),array(2),array(3),array(4),array(5),
          array(6),array(7),array(8))).toDF()
      println("记录条数:" + cashPlanDF.count())

      //结果写入MongoDB
      println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date) +":开始写入MongoDB")
      cashPlanDF.write.mode("append").mongo()
      println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date) +":成功写入MongoDB")

      //当处理完成后，手动提交偏移量
      println("处理完成后提交偏移量!")
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
case class CashPlanRecord2(_id:String,MachineNo:String,TermPaymentDay:String,
                          Currency:String,BranchNo:String,BLOperTellerID:String,
                          BLOperTime:String,CashNum:String,FaceValue:String)
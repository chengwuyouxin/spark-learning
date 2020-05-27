import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

object Ex3_Save2Redis {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf()
      .setAppName("CashPlanLoad")
      .set("spark.streaming.stopGracefullyOnShutdown","true")//程序关闭时会先处理完接受到的数据
      .set("spark.streaming.backpressure.enabled", "true") //激活削峰功能,sparkstreaming可以根据系统处理
      //处理能力来调节获取的流量的大小，该流量的最大值由maxRatePerPartition来设置,一般都会小于该值
      .set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值，需要设置backpressure为true
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val ssc=new StreamingContext(conf,Seconds(5))

    val kafkaParams=Map[String,Object](
      "bootstrap.servers"->"dn01:9092,dn02:9092,dn03:9092",
      "key.deserializer" -> classOf[IntegerDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CPRConsumerGroup",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    val topic=Array("CashPlanRecord")
    val stream=KafkaUtils.createDirectStream[Integer,String](
      ssc,
      PreferConsistent,
      Subscribe[Integer,String](topic,kafkaParams)
    )

    //sparkstreaming通过Direct方式创建DStream，其rdd的partition数量和topic的partition数量一样
    stream.foreachRDD{rdd=>
      //获取topic的每个partition的偏移量
      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition{ iter=>
        val o:OffsetRange=offsetRanges(TaskContext.get.partitionId())
        println("Kafka记录分区等信息：" + o.topic+","+o.partition+","+o.fromOffset+","+o.untilOffset)
      }

      //业务处理逻辑
      println("rdd的分区数量:"+rdd.partitions.size)
      val spark=SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val cashPlanDF=rdd.map(_.value)
        .map(line => line.split(","))
        .map(items => CashPlanRecord(items(0),items(1),items(2),items(3),items(4),items(5),
          items(6),items(7),items(8))).toDF()
      println("记录条数:" + cashPlanDF.count())

      //写入redis
      println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date) +":开始写入redis")
      cashPlanDF.foreachPartition(partition =>{
        val jedis=JedisConnectionPool.pool.getResource
        val map=new java.util.HashMap[String,String]();
        partition.foreach(record =>{
          map.put("MachineNo",record(1).toString)
          map.put("currency",record(3).toString)
          map.put("operTime",record(6).toString)
          map.put("cashNum",record(7).toString)
          map.put("faceValue",record(8).toString)
          jedis.hmset("cashplan:"+record(0),map)
        })
        jedis.close();
      })
      println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date) +":成功写入redis")

      //当处理完成后，手动提交偏移量
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

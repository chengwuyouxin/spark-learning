import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Ex12_SaveOffset2Redis {
  def main(args:Array[String]): Unit ={
    val conf=new SparkConf().setMaster("local[2]")
      .setAppName("Ex12_SaveOffset2Redis")
    val ssc=new StreamingContext(conf,Seconds(5))

    val kafkaProp=Map[String,Object](
      "bootstrap.servers" -> "dn01:9092,dn02:9092,dn03:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.IntegerDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "CPRConsumerGroup",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    val topic=Array("CashPlanRecord")
    val fromOffsets=getLastCommitedOffsets(topic(0),1)
    fromOffsets.foreach(println)
    val stream=KafkaUtils.createDirectStream[Integer,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[Integer,String](fromOffsets.keys.toList,kafkaProp,fromOffsets)
    )

    stream.foreachRDD { rdd =>
      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val jedis=JedisConnectionPool.pool.getResource
      val p=jedis.pipelined()
      p.multi()

      //业务逻辑
      val spark=SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val cashPlanDF=rdd
        .map(_.value())
        .map(_.split(","))
        .map(array => CashPlanRecord(array(0),array(1),array(2),array(3),array(4),array(5),
          array(6),array(7),array(8))).toDF()
      println("记录数："+cashPlanDF.count())
      println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date) +":开始写入mysql")
      cashPlanDF.write
        .mode("append")
        .format("jdbc")
        .option("url","jdbc:mysql://22.11.97.142:3306/cash")
        .option("dbtable","cashplan")
        .option("user","root")
        .option("password","admin")
        .save()
      println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date) +":成功写入mysql")

      //更新Redis中的offset
      offsetRanges.foreach( offsetRange =>{
        val topic_Partition_key=offsetRange.topic+"_"+offsetRange.partition
        p.set(topic_Partition_key,String.valueOf(offsetRange.untilOffset))
      })

      p.exec()
      p.sync()
      jedis.close()
    }
    ssc.start()
    ssc.awaitTermination()
  }
  def getLastCommitedOffsets(topic:String,partitionsNum:Int):Map[TopicPartition,Long]={
    val jedis=JedisConnectionPool.pool.getResource()
    val fromOffsets=collection.mutable.HashMap.empty[TopicPartition,Long]
    for(partition <- 0 to partitionsNum-1){
      val topic_partition_key=topic+"_"+partition
      val lastSaveOffset=jedis.get(topic_partition_key)
      val lastOffset=if(lastSaveOffset==null) 0L else lastSaveOffset.toLong
      fromOffsets += (new TopicPartition(topic,partition) -> lastOffset)
    }
    jedis.close()
    fromOffsets.toMap
  }
}

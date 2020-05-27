import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

object Ex1_CheckPoint {
  def main(args:Array[String]):Unit={
      val checkpointDirectory="hdfs://22.11.97.142:8020/checkpoint/cashstore";
      val ssc=StreamingContext.getOrCreate(checkpointDirectory,
        () => functionToCreateContext(checkpointDirectory));
      ssc.start()
      ssc.awaitTermination()
    }

    //checkpoint的作用：
    //1.保存MeteDate,包括应用程序配置/DStream Operation/未处理完成的batch,可以让应用程序宕机后重启
    //2.使用带状态的算子时保存状态,如updateStateByKey和reduceByKeyAndWindow

    //使用checkpoint时需要注意以下几点：
    //1.当使用checkpoint时，业务逻辑要写在functionToCreateContext中，不然当应用宕机重启时会失败
    //2.当应用程序宕机后重启，不会重新创建StreamingContext，而是读取checkpoint路径下的信息继续执行程序
    //  如果需要更新应用程序，此时我们需要删除checkpoint路径，然后再重启

    //更新应用程序的方式
    //1.新旧程序并行一段时间
    //2.首先优雅地关闭旧应用，启动新的应用程序,此时如果有checkpoint路径，需要先删除该路径再启动新应用程序

    //如何优雅的关闭sparkstreaming应用
    //1.设置参数 set("spark.streaming.stopGracefullyOnShutdown","true")
    def functionToCreateContext(checkpointDirectory: String):StreamingContext={
      println("create new SparkStreamingContext!")
      val conf=new SparkConf()
        .setAppName("CashStore")
        .set("spark.streaming.stopGracefullyOnShutdown","true")//程序关闭时会先处理完接受到的数据
        .set("spark.streaming.backpressure.enabled", "true") //激活削峰功能,sparkstreaming可以根据系统处理
         //处理能力来调节获取的流量的大小，该流量的最大值由maxRatePerPartition来设置,一般都会小于该值
        .set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值，需要设置backpressure为true
        .set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
        .set("hive.metastore.uris","thrift://22.11.97.142:9083")
        .set("spark.sql.crossJoin.enabled", "true")
        .set("spark.sql.shuffle.partitions","200")
        .set("spark.sql.broadcastTimeout","3600")
        .set("hive.exec.dynamic.partition.mode","nonstrict")
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.max.dynamic.partitions", "10000")
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

      val ssc=new StreamingContext(conf,Seconds(10))
      ssc.checkpoint(checkpointDirectory)
      ssc.sparkContext.setLogLevel("Error")

      val kafkaProp=Map[String,Object](
        "bootstrap.servers" -> "dn01:9092,dn02:9092,dn03:9092",
        "key.deserializer" -> "org.apache.kafka.common.serialization.IntegerDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "group.id" -> "CPRConsumerGroup3",
        "auto.offset.reset" -> "earliest",//earliest.latest,none
        //当存在已经提交的offset时，earliest和latest都是从offset开始消费数据
        //当不存在提交的offset时，earliest从头开始消费数据,latest从最新开始消费数据
        "enable.auto.commit" -> (false:java.lang.Boolean) //手动异步提交偏移量

      )
      val topic=Array("CashPlanRecord")
      val stream=KafkaUtils.createDirectStream[Integer,String](
        ssc,
        PreferConsistent,
        Subscribe[Integer,String](topic,kafkaProp)
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
        import spark.implicits._;
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

        //当前批次数据处理完成后，手动提交偏移量
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
      ssc
    }
}

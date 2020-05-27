package com.lpq.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
知识点：
1.grouping sets/cube/rollup的使用
2.spark sql对多维分析数据的expand处理
3.spark sql对count（distinct）的expand处理
 */

object MultiDimAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()

    import spark.implicits._
    val orders=Seq(
      MemberOrderInfo("深圳","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("深圳","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("深圳","钻石会员","钻石会员3个月",70),
      MemberOrderInfo("深圳","钻石会员","钻石会员12个月",300),
      MemberOrderInfo("深圳","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("深圳","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("深圳","铂金会员","铂金会员6个月",120),
      MemberOrderInfo("深圳","黄金会员","黄金会员1个月",15),
      MemberOrderInfo("深圳","黄金会员","黄金会员1个月",15),
      MemberOrderInfo("深圳","黄金会员","黄金会员3个月",45),
      MemberOrderInfo("深圳","黄金会员","黄金会员12个月",180),
      MemberOrderInfo("北京","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("北京","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("北京","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("北京","黄金会员","黄金会员3个月",45),
      MemberOrderInfo("上海","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("上海","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("上海","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("上海","黄金会员","黄金会员3个月",45)
    )
    //把seq转换成DataFrame
    val memberDF:DataFrame =orders.toDF()
    //把DataFrame注册成临时表
    memberDF.createOrReplaceTempView("orderTempTable")


    //一.grouping sets 定义多个维度进行聚合
    // 1.grouping sets(area,memberType,product) 分别对三个维度进行聚合
//    spark.sql("select area,memberType,product,sum(price) as total " +
//      "from orderTempTable " +
//      "group by area,memberType,product grouping sets(area,memberType,product)" +
//      "order by area,memberType,product").show()

    // 2.grouping sets((area,memberType),(area,product)) 对(area,memberType),(area,product)两个维度组进行聚合
//    spark.sql("select area,memberType,product,sum(price) as total " +
//      "from orderTempTable " +
//      "group by area,memberType,product " +
//      "grouping sets((area,memberType),(area,product))" +
//      "order by area,memberType,product").show()

    //rollup
    //group by area,membertype,product with rollup 依次对(area),(area,membertype),(area,membertype,product)进行分组
//    spark.sql("select area,membertype,product,sum(price) as total " +
//      "from orderTempTable " +
//      "group by area,membertype,product " +
//      "with rollup " +
//      "order by area,memberType,product").show(100)

    //cube
    //group by A,B,C with cube，则首先会对(A、B、C)进行group by，然后依次是(A、B)，(A、C)，(A)，(B、C)，(B)，( C)，最后对全表进行group by操作
//    spark.sql("select area,membertype,product,sum(price) as total " +
//      "from orderTempTable " +
//      "group by area,membertype,product " +
//      "with cube " +
//      "order by area,memberType,product").show(100)

//二. 数据expand的问题,grouping sets/cube/rollup，数据expand的倍数时分组的倍数
    //本次查询有三个分组操作，从物理查询计划可以看到，expand了三倍
//    spark.sql("select area,memberType,product,sum(price) as total " +
//      "from orderTempTable " +
//      "group by area,memberType,product " +
//      "grouping sets(area,memberType,product)" +
//      "order by area,memberType,product").explain()

    //三. spark sql对count(distinct)的处理
    //从物理查询计划可以看到，spark sql对数据expand了三份，每份中只有一个维度，因此只要先进行group by操作，再count即可
    //这种做法可以加快执行速度，但是由于数据扩张了几倍，因此需要综合权衡
    spark.sql("select count(distinct area),count(distinct memberType),count(distinct(product))" +
      " from orderTempTable").explain()

  }

}

case class MemberOrderInfo(area:String,memberType:String,product:String,price:Int)

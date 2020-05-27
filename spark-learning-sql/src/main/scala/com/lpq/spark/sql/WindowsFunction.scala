package com.lpq.spark.sql

import org.apache.spark.sql.SparkSession


/*
一，函数
1.排序函数
row_num() 分组内进行排序，当排序值重复时，按照表中顺序进行排序，不会出现重复值
rank() 分组内排序，排序值重复时，排名相同，后续排名出现空缺
dense_rank() 分组内排序，排序值重复时，排名相同，后续排名不会出现空缺
2.聚合函数
sum/min/max/mean
3.分析函数
(1) ntile(n) 用于将数据按照顺序分成n份，返回当前切片值，当切片不均匀时默认增加第一个分片的分布
使用场景：
如一年中，统计出工资前1/5之的人员的名单，使用NTILE分析函数,把所有工资分为5份，为1的哪一份就是我们想要的结果.
sale前20%或者50%的用户ID
(2) lead 统计窗口内往下n行的值 lead(col,n,default) col-列名 n-往下n行，默认为1  default-没有记录时的默认值

（3）lag（col,n,default） 统计窗口内往上n行的值

(4) first_value 分组内排序后，截止到当前行的第一行

（5） last_value 分组内排序后，截至到当前行的最后一行

（6） cume_dist 小于等于当前行的行数/分组内总行数

(7) percent_rank分组内当前行的RANK值-1/分组内总行数-1
二.窗口
1.partition by
2.order by
3.rows between A and B
PRECEDING：往前
FOLLOWING：往后
CURRENT ROW：当前行
UNBOUNDED：起点，
UNBOUNDED PRECEDING 表示从前面的起点，
UNBOUNDED FOLLOWING：表示到后面的终点

实际使用时，窗口和函数一般会同时出现

三.剔除特殊数据后进行排序

 */
object WindowsFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._

    val view = Seq(
      View("cookie1","2015-04-15",4),
      View("cookie1","2015-04-12",7),
      View("cookie1","2015-04-14",2),
      View("cookie1","2015-04-16",4),
      View("cookie1","2015-04-10",1),
      View("cookie1","2015-04-13",3),
      View("cookie1","2015-04-11",5)
    ).toDF()
    view.createOrReplaceTempView("view_table")

    //1.排序函数 此处没有写窗口，从执行计划中可以看到，默认窗口为ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
//    spark.sql("select cookieid,create_time,pv," +
//      "row_number() over(partition by cookieid order by pv) as row_number, " +
//      "rank() over(partition by cookieid order by pv) as rank, " +
//      "dense_rank() over(partition by cookieid order by pv) as dense_rank " +
//      "from view_table").explain(true)

    //2.聚合函数
    //pv1--默认从起点到当前行
    //pv2 --从起点到当前行
    //pv3--分组内所有行，相当于group by
    //pv4--往前3行到当前行
    //pv5--往前3行到往后1行
    //pv6--从当前行到分组末尾
//    spark.sql("select cookieid,create_time,pv," +
//      "sum(pv) over(partition by cookieid order by create_time) as pv1," +
//      "sum(pv) over(partition by cookieid order by create_time " +
//        "Rows between unbounded preceding and current row) as pv2," +
//      "sum(pv) over(partition by cookieid) as pv3," +
//      "sum(pv) over(partition by cookieid order by create_time " +
//        "Rows between 3 preceding and current Row) as pv4," +
//      "sum(pv) over(partition by cookieid order by create_time " +
//        "Rows between 3 preceding and 1 Following) as pv5," +
//      "sum(pv) over(partition by cookieid order by create_time " +
//        "Rows between current row and unbounded following) as pv6 " +
//      "from view_table").show()

    //ntile 用于将数据按照顺序分成n份，返回当前切片值，当切片不均匀时默认增加前面分片的分布
//    使用场景：
//    1.如一年中，统计出工资前1/5之的人员的名单，使用NTILE分析函数,把所有工资分为5份，为1的哪一份就是我们想要的结果.
//    2.sale前20%或者50%的用户ID
//    spark.sql("select cookieid,create_time,pv," +
//      "ntile(2) over(partition by cookieid order by create_time) as nt2," +
//      "ntile(3) over(partition by cookieid order by create_time) as nt3," +
//      "ntile(4) over(partition by cookieid order by create_time) as nt4 " +
//      "from view_table").show()

    // lead 统计窗口内往下n行的值 lead(col,n,default) col-列名 n-往下n行，默认为1  default-没有记录时的默认值,默认是null
//    spark.sql("select cookieid,create_time,pv," +
//      "lead(create_time,1,'2020-4-29') over(partition by cookieid order by create_time) as time1," +
//      "lead(create_time,2) over(partition by cookieid order by create_time) as time2 " +
//      "from view_table").show()

    //lag（col,n,default） 统计窗口内往上n行的值
//    spark.sql("select cookieid,create_time,pv," +
//      "lag(create_time,1,'2020-4-30') over(partition by cookieid order by create_time) as lag1," +
//      "lag(create_time,2) over(partition by cookieid order by create_time) as lag2 " +
//      "from view_table").show()

//   first_value(col) 分组内排序后，截止到当前行的第一行
//   last_value(col) 分组内排序后，截至到当前行的最后一行
//    spark.sql("select cookieid,create_time,pv," +
//      "first_value(pv) over(partition by cookieid order by create_time) as first_value," +
//      "last_value(pv) over(partition by cookieid order by create_time) as last_value " +
//      "from view_table").show()
    //要想获得获取分组内的最后一行，order by默认是升序排列，只需降序排列取第一个值即可
//    spark.sql("select cookieid,create_time,pv," +
//      "first_value(pv) over(partition by cookieid order by create_time) as first_value," +
//      "last_value(pv) over(partition by cookieid order by create_time) as last_value," +
//      "first_value(pv) over(partition by cookieid order by create_time desc) as last_value_in_group " +
//      "from view_table").show()

    //cume_dist小于等于当前行的行数/分组内总行数
//    spark.sql("select cookieid,create_time,pv," +
//      "dense_rank() over(partition by cookieid order by create_time) as dense_rank," +
//      "sum(1) over(partition by cookieid) as sum," +
//      "cume_dist() over(partition by cookieid order by create_time) as cume_dist " +
//      "from view_table").show()

    //percent_rank分组内当前行的RANK值-1/分组内总行数-1
    spark.sql("select cookieid,create_time,pv," +
      "rank() over(partition by cookieid order by create_time) as rank," +
      "sum(1) over(partition by cookieid) as sum," +
      "percent_rank() over(partition by cookieid order by create_time) as percent_rank " +
      "from view_table").show()


    //三.剔除特殊数据后进行排序
//    val visitRecord = Seq(
//      VisitRecord(1,"close",1,1),
//      VisitRecord(1,"close",2,2),
//      VisitRecord(1,"close",3,2),
//      VisitRecord(1,"close",4,4),
//      VisitRecord(1,"close",5,5),
//      VisitRecord(2,"cup",1,1),
//      VisitRecord(2,"cup",1,2),
//      VisitRecord(2,"cup",3,3),
//      VisitRecord(2,"cup",1,4),
//      VisitRecord(2,"cup",1,4),
//      VisitRecord(2,"cup",1,6)
//    )
//    visitRecord.toDF().createOrReplaceTempView("visit_table")

    //ProdType为3的是广告，需要剔除该类商品后进行排序
//    spark.sql("select userid,ProdName,ProdType,rank as natural_rank," +
//      "if(ProdType!=3,rank() over(partition by userid order by rank),null) as rank " +
//      "from visit_table").show()

//    |userid|ProdName|ProdType|natural_rank|rank|
//    +------+--------+--------+------------+----+
//    |     1|   close|       1|           1|   1|
//      |     1|   close|       2|           2|   2|
//      |     1|   close|       3|           2|null|
//      |     1|   close|       4|           4|   4|
//      |     1|   close|       5|           5|   5|
//      |     2|     cup|       1|           1|   1|
//      |     2|     cup|       1|           2|   2|
//      |     2|     cup|       3|           3|null|
//      |     2|     cup|       1|           4|   4|
//      |     2|     cup|       1|           4|   4|
//      |     2|     cup|       1|           6|   6|
//      +------+--------+--------+------------+----+
    //输出结果中最后类型为的3的并未剔除，分析一下物理执行计划
//    spark.sql("select userid,ProdName,ProdType,rank as natural_rank," +
//      "if(ProdType!=3,rank() over(partition by userid order by rank),null) as rank " +
//      "from visit_table").explain()
    //从物理执行计划可以看到，if判断是再project阶段执行，因此结果不符合预期

//    spark.sql("select userid,ProdName,ProdType,rank as natural_rank," +
//      "if(ProdType != 3,rank() over(partition by if(ProdType != 3,userid,-9999) order by rank),null) as rank " +
//      "from visit_table order by userid,natural_rank").show()



  }
}

case class VisitRecord(userId:Int,ProdName:String,ProdType:Int,Rank:Int)
case class View(cookieid:String,create_time:String,pv:Int)

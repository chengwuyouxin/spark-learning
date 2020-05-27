import scala.math._

object HighLevelFunction {
  def main(args:Array[String]): Unit ={
//    val num=3.14
//
//    //_ 表示要将函数给fun
//    val fun=ceil _
//    val fun2:(Double)=>Double =ceil
//    println(fun(num))
//    println(fun2(num))
//
//    //匿名函数
//    val fun3= (x:Double) => x*3
//    println(fun3(1.0))
//    val a=Array(1.2,3.4,5.6).map((x:Double)=>x*3)
//    //数组遍历
//    for(i <- 0 until a.length)
//      println(a(i))
//    a.foreach(println)

//    //high-order function
////    def valueAtOneQuarter(f:(Double)=>Double)=f(0.25)
//    def valueAtOneQuarter(f:(Double)=>Double):Double ={
//      f(0.25)
//    }
//
//    //打印方法，注意后面有个下划线，表名这是一个函数，而不是变量
//    println(floor _)
//    println(ceil _)
//    println(valueAtOneQuarter(floor _))
//    println(valueAtOneQuarter(ceil _))

    //产生函数的函数
//    def mulBy(factor:Double)=(x:Double)=>factor*x
//    val function1=mulBy(5)
//    print(function1(10))

    //currying
    def mulOneAtATime(x:Int)(y:Int):Int=x*y
    println(mulOneAtATime(3)(4))

    val mulOneAtATime2=(x:Int)=>(y:Int)=>x*y
    println(mulOneAtATime2(3)(4))



  }
}

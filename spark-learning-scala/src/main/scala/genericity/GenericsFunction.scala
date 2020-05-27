package genericity

/**
  * @author liupengqiang
  * @date 2020/5/12
  * 泛型函数
  */
object GenericsFunction {
  def main(args: Array[String]): Unit = {
    //直接调用
    println(getMiddle(Array("spark","hadoop","flink")))

    //确定泛型函数的类型
    val f = getMiddle[String] _
    println(f(Array("hdfs","hive")))
  }

  def getMiddle[T](a:Array[T]):T = {
    a(a.length / 2)
  }
}

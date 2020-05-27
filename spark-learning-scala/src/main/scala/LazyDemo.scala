/**
  * @author liupengqiang
  * @date 2020/5/14
  */
object LazyDemo {
  def main(args: Array[String]): Unit = {
    val a = new LazyClass
//    println(a.lazyprop)
    print(a.invokelazyprop())
  }
}

class LazyClass{

  def invokelazyprop() = lazyprop

  lazy val lazyprop = {
    println("lazy property is invoked!")
    "lazy_prop_value"
  }
}

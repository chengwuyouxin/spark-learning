package chapter6

/**
  * @author liupengqiang
  * @date 2020/5/13
  */
object Pencil {
  //对象的构造器仅在对象第一次被调用时执行，仅执行一次
  println("Object Pencil's constructor is invoked!")

  private var lastNumber = 0

  def newUniqueNum()={
    lastNumber += 1
    lastNumber
  }
}


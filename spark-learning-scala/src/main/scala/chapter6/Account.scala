package chapter6

/**
  * @author liupengqiang
  * @date 2020/5/13
  */
class Account {

}

//通常apply方法用于返回 伴生类 的对象
object Account{
  //Account()
  def apply(): Unit ={
    println("Object's apply() method is invoked!")
  }

  //Account(1.2) 会调用此方法
  def apply(initialBalance:Double): Unit ={
    println("Object's apply(initialBalance:Double) is invoked!")
  }
}

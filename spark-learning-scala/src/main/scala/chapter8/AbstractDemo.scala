package chapter8

/**
  * @author liupengqiang
  * @date 2020/5/15
  *      1.定义抽象方法只需给出方法名和返回值，不用给出方法体
  *      2.如果类中有一个或多个抽象方法，则类必须定义为抽象类
  *      3.抽象类中也可以定义非抽象方法
  *      4.子类实现父类的抽象方法，使用extends关键字；实现抽象方法使用override；如果子类是非抽象类，则必须实现全部的抽象方法;
  *        如果不想实现，可以写成 def abstractmethod: Unit = ??? 方式
  *      5.抽象类不可以被实例化
  */
object AbstractDemo {
  def main(args: Array[String]): Unit = {
    val m = new Manage("lpq",29,4555)
    println(m.getName)
  }
}

abstract class Employee(val name:String,val age:Int){
  //定义一个抽象方法
  def getName:String

  //定义第二个抽象方法
  def abstractmethod:Unit

  //非抽象方法
  def getAge:Int = age
}

class Manage(name:String,age:Int,val salary:Double) extends Employee(name,age){
  def getName:String = {
    return name
  }

  def abstractmethod: Unit = ???
}

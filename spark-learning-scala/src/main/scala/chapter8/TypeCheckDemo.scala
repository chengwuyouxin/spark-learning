package chapter8

/**
  * @author liupengqiang
  * @date 2020/5/15
  *      C1 类名  obj 类对象
  *      1.classOf[C1]           类似与 java中的   C1.class            返回类对象
  *      2.obj.isInstanceOf[C1]                   obj instanceOf C1  判断obj是否是C1及其子类的实例
  *      3.obj.asInstanceOf[C1]                   (C1) obj           将obj转换成C1类的实例
  *      4.obj.getClass()                         obj.getClass       获取类对象
  */
object TypeCheckDemo {
  def main(args: Array[String]): Unit = {
    var jim = new Student(29,"lpq",34)
    println(jim.isInstanceOf[Student])
    println(jim.isInstanceOf[Person])
    println(jim.isInstanceOf[SubStudent])

    //将Student类型的对象向上转型成Person类，但s仍然是Student类型
    if(jim.isInstanceOf[Person]){
      val s = jim.asInstanceOf[Person]
      s match {
        case a:Student => println("Student instance")

        case b:Person => println("Person instance")

        case _ => println("default")
      }
      print(s.getClass)
    }
  }
}

class Person(var age:Int,val name:String){
  def printAge = {
    println(age)
  }
  def printName: Unit = {}

}

class Student(age:Int,name:String,var salary:Double) extends Person(age,name){
  //重写父类的方法，必须使用override关键字
  override def printAge={
    println("Student's printAge "+age)
  }

}

class SubStudent(age:Int,name:String,salary:Double) extends Student(age,name,salary){

}


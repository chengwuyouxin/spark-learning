package chapter8

/**
  * @author liupengqiang
  * @date 2020/5/15
  */
object TypeDemo {
  def main(args: Array[String]): Unit = {

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

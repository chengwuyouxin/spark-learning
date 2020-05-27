package chapter5

/**
  * @author liupengqiang
  * @date 2020/5/12
  */
object Main {
  def main(args: Array[String]): Unit = {
    val p1 = new Person
    p1.age = 2
    println(p1.age)

    val s = new Student
    val c = new Child("ljx",5)
    c.age = 6
    c.name = "kk"
    println(c.age)
    println(c.name)
  }
}
class Person {
  //生成私有字段age 共有的getter (age)和setter(age_$eq)方法
  var age :Int = 0

  //生成私有字段age 私有的getter和setter方法
  private var name:String = "lpq"
}

//由于没有显示定义主构造器，默认有一个无参的主构造器
class Student{
  private var name:String = ""
  private var age =0

  println("main constructor this() is invoked!")

  //辅助构造器
  def this(name:String){
    this()
    this.name = name
    println("constructor this(name:String) is invoked)!")
  }
  //辅助构造器
  def this(name:String, age:Int){
    this(name)
    this.age = age
    println("constructor this(name:String, age:Int) is invoked)!")
  }
}

// 1.定义了带有两个参数的主构造器
// 2.主构造器的参数被编译成字段
// 3.主构造器会执行类定义中的所有语句
class Child(var name:String, var age:Int){
  println("main constructor this(name:String, age:Int) is invoked!")
}
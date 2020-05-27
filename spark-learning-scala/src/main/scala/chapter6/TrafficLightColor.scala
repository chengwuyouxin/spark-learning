package chapter6

/**
  * @author liupengqiang
  * @date 2020/5/13
  */

object EnumerationDemo{
  def main(args: Array[String]): Unit = {
    val tc = TrafficLightColor.Red
    print(tc.getClass)

    tc match {
      case TrafficLightColor.Red => println("Match Red")
      case _ => print(" ")
    }

    println(TrafficLightColor.Red)
    println(TrafficLightColor.Red.id)
    println(TrafficLightColor.Yellow)
    println(TrafficLightColor.Yellow.id)
    println(TrafficLightColor.Green)
    println(TrafficLightColor.Green.id)
  }
}

object TrafficLightColor extends Enumeration{
//  val Red,Yellow,Green = Value;
  val Red  = Value(0,"Stop")
  val Yellow = Value(1,"Wait")
  val Green = Value(2,"Go")
}


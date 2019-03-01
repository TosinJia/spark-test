package basic.scala.itstart01.d04

import demo.Person51

object ScalaDemo {
  def main(args: Array[String]): Unit = {
    //constructor Person4 in class Person4 cannot be accessed in object ScalaDemo
    //被private修饰的主构造器，对外没有访问权限
//    val p4 = new Person4("h",88)
//    println("name:"+p4.name+"\thigh:"+p4.high)

    val p5 = new Person5("tosin",19,170)
    println("name:"+p5.name+"\thigh:"+p5.high)

    val p51 = new Person51("tosin",19,170)
    println("name:"+p51.name+"\thigh:"+p51.high)
  }
}

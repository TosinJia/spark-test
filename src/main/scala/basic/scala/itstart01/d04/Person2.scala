package basic.scala.itstart01.d04

/**
  * 类名后 定义主构造器
  * */
class Person2(name:String, age:Int) {}

object Test2 extends App {
  val p = new Person2(name="tosin", age = 34)
  println(p)
  println(p.toString)
  val p2 = new Person2("tosin", 34)
  println(p)
}

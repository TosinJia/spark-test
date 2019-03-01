package basic.scala.itstart01.d04

/**
  * 定义抽象类
  * */
abstract class AbstractDemo {
  def eat(food:String)
  def sleep(how:String): Unit = {
    println(s"$how -> 很香")
  }
}

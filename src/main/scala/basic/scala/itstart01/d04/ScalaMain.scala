package basic.scala.itstart01.d04

/**
  * 在scala中object是一个单例对象
  * 在scala中object中定义的成员变量和方法都是静态的
  * 可以通过 类名. 来进行调用
  * */
object ScalaMain {
  def main(args: Array[String]): Unit = {
    println(ScalaTest.name)
    ScalaTest.sleep("good")

    //ScalaC 不能直接调用
  }
}

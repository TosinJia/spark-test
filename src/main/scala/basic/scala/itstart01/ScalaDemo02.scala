package basic.scala.itstart01

object ScalaDemo02 {
  def main(args: Array[String]): Unit ={
    println(m1(2, 8))
    println(m2(3, 10))
  }

  //加法
  def m1(a:Int, b:Int):Int={
    a+b
  }
  //方法体{}可以省略
  def m2(a:Int, b:Int): Int = a + b
}

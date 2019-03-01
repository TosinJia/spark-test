package basic.scala.itstart01.d02

object DefaultParam {
  def sum(a: Int = 3, b: Int = 7): Int = {
    a+b
  }

  def main(args: Array[String]): Unit= {
    //如果传递了参数，则使用传递的值。如果不传递参数，则使用默认值。
    println(sum(1,2))
    println(sum(2))
    println(sum())
  }
}

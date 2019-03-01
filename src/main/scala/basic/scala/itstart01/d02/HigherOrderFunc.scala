package basic.scala.itstart01.d02

object HigherOrderFunc {
  def person1(x: Int): String = "I am " + x.toString + " years old."

  def person2(x: Int): String = {
    "I am " + x.toString + " years old."
  }
  //将其他函数作为参数，或其结果是函数的函数
  def getPerson(p: Int => String, a: Int): String = {
    //函数p，参数a
    p(a)
  }

  def main(args: Array[String]): Unit = {
    println(getPerson(person1, 34))
    println(getPerson(person2, 34))
  }
}

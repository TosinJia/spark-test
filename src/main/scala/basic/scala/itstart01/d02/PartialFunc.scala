package basic.scala.itstart01.d02

object PartialFunc {
  //定义函数
  def func1(name: String): Int = {
    if(name.equals("tosin")) 34 else -1
  }

  //定义偏函数
  def func2:PartialFunction[String, Int] = {
    //如果使用了偏函数，必须用case
    case "tosin" => 34
    //如果是其他
    case _ => -1
  }

  def main(args: Array[String]): Unit ={
    println(func1("tosin"));
    println(func2("tosin"))
  }
}

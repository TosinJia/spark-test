package basic.scala.itstart01.d02

object VariableParam {
  //可变参数
  def sum(ints: Int*): Int = {
    var sum = 0
    for(i <- ints){
      sum += i
    }
    sum
  }

  //参数类型不一致的可变参数
  def setName(params: Any*): Any = {
    return params
  }

  def main(args: Array[String]): Unit = {
    println(sum(1,2))
    println(sum(1,2,3,4,5))
    println(setName("tosin",18,34))
  }
}

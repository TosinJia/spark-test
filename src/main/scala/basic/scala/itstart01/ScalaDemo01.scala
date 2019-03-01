package basic.scala.itstart01

object ScalaDemo01 {
  def main(args: Array[String]): Unit = {
    //块表达式
    val a = 1
    val rs = {
      if(a>2){
        1
      }else if(a<2){
        -1
      }else{
        0
      }
    }

    println(a)
  }
}

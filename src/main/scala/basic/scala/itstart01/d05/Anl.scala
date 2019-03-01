package basic.scala.itstart01.d05

trait Anl {
  //定义特质时可以不指定类型
  type T
  def sleep(str: T): Unit ={
    println(str)
  }
}

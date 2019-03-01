package basic.scala.itstart01.d04

trait Animal {
  //定义未实现的方法
  def eat(name:String)

  //定义实现的方法
  def sleep(name:String):Unit = {
    println(f"$name -> 在睡觉")
  }
}

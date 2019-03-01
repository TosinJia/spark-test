package basic.scala.itstart01.d05

object Pig extends Animal {
  //val name:String = "tosin"
  final var name:String = "Pig" //以var为准
  var age:Int = 8

  //重写eat方法
  override def eat(name: String): Unit = {
    println(s"$name => 吃吃吃")
  }
  //Method sleep cannot override final member
  override def sleep(name: String): Unit = {
    println(s"$name => 做梦吃鸡")
  }

  def main(args: Array[String]): Unit = {
    Pig.eat("pig")
    Pig.sleep("pig")

    Pig.name = "bigPig"
    println(Pig.name)
    Pig.age = 20
    println(Pig.age)
  }
}

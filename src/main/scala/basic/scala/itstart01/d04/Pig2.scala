package basic.scala.itstart01.d04

/**
  * with 特质 混入特质
  * */
object Pig2 extends Animal with Running {
  override def eat(name: String): Unit = {
    println(f"$name => 吃猪食")
  }

  override def sleep(name: String): Unit = {
    println(s"$name => 长膘")
  }

  def main(args: Array[String]): Unit = {
    Pig2.eat("pig")
    Pig2.sleep("pig")
    Pig2.how("pig")
  }
}

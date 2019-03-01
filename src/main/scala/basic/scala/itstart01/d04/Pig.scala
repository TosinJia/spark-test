package basic.scala.itstart01.d04

object Pig extends Animal {
  override def eat(name: String): Unit = {
    println(s"$name => 在吃饭")
  }

  override def sleep(name: String): Unit = {
    println(f"$name => 在做梦")
  }

  def main(args: Array[String]): Unit = {
    Pig.eat("j")
    Pig.sleep("t")
  }
}

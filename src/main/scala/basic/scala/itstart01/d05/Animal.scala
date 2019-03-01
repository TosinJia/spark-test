package basic.scala.itstart01.d05

//final trait Animal {  //final modifier not allowed with trait
trait Animal {
  def eat(name:String)

  def sleep(name:String): Unit = {
//final def sleep(name:String): Unit = {
    println(s"$name -> 睡得天花乱坠")
  }
}
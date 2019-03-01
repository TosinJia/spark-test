package basic.scala.itstart01.d04

//class AbstractImpl extends AbstractDemo {
//object AbstractImpl extends AbstractDemo {
//继承抽象类可以在继承特质，但是抽象类写在前，用with连接 extends 抽象类 with 特质
object AbstractImpl extends AbstractDemo with Running {
  override def eat(food: String): Unit = {
    //ctr+i
    println(s"$food => 吃猪蹄")
  }

  def main(args: Array[String]): Unit = {
    AbstractImpl.eat("tosin")
  }
}

package basic.scala.itstart01.d02

object ZFBToPay {
  var money = 1000

  //吃一次饭花50
  def eat():Unit = {
    money = money - 50
  }

  //余额
  def balance(): Int = {
    //调用一次eat方法
    eat()
    money
  }

  //传值调用
  def printMoney1(x: Int): Unit = {
    for(a <- 1 to 5){
      println(f"你的余额为：$x");
    }
  }

  //传名调用
  def printMoney2(x: => Int):Unit = {
    for(a <- 1 to 5){
      println(f"你的余额为：$x")
    }
  }

  def main(args: Array[String]): Unit = {
//    eat()
//    balance()
    //传值调用
    printMoney1(balance)
    printMoney1(balance())
    //传名调用
    printMoney2(balance)
    printMoney2(balance())
  }
}

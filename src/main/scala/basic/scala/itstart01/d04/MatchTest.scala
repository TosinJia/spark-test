package basic.scala.itstart01.d04

object MatchTest {
  def main(args: Array[String]): Unit = {
    def strMatch(str:String):Unit = str match {
      case "tosin" => println("很帅")
      case "reba" => println("很美")
      case _ => println("who?")
    }
    strMatch("hunter")
    strMatch("tosin")

    def arrayMatch(arr:Any): Unit = arr match {
      case Array(1) => println("Array(1)")
      case Array(1,2) => println("Array(1,2)")
      case _ => println("其他")
    }
    arrayMatch(Array(1))
    arrayMatch(Array(2,2))

    def tuple(tuple: Any): Unit = tuple match {
      case (1,_) => println("第一个元素为1，第二个元素任意")
      case ("hunter", 18) => println("(\"hunter\", 18)")
      case _ => println("其他")
    }
    tuple(("hunter",18))
    tuple(("hunter",17))
  }
}

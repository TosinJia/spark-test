package basic.scala.test.commerce

object Test01 {
  def main(args: Array[String]): Unit = {
    val sentence = "一首现代诗《笑里藏刀》 哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈刀哈哈哈哈哈哈哈哈哈哈"
    val map = (Map[Char,Int]() /: sentence)(
      (m, c) =>
        m + (c -> (m.getOrElse(c,0)+1))
    )
    println(map)

  }
}

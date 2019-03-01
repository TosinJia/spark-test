package basic.scala.itstart01.d04

/**
  * 定义类
  * */
class Person1 {
  //定义姓名、年龄 不确定，只想初始化
  var name:String = _
  var age:Int = _
}

//继承App特质，可以不写main
object Test extends App{
  val p = new Person1
  p.name = "jia"
  p.age = 16

  println(p.name)
  println(p.age)
}
package basic.scala.itstart01.d04

/**
  * 类的构造器访问权限
  * private 私有
  * 主构造器设置为私有
  * */
class Person4 private (var name:String, age:Int) {
  var high:Int = _
  private def this(name:String, age:Int, high:Int){
    this(name, age)
    this.high = high
  }
}
